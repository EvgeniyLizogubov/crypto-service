package io.github.evgeniylizogubov.cryptoservice.service;

import io.github.evgeniylizogubov.cryptoservice.entity.PriceEntity;
import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.model.CryptocurrencyPrice;
import io.github.evgeniylizogubov.cryptoservice.provider.PriceProvider;
import io.github.evgeniylizogubov.cryptoservice.repository.PriceReactiveRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PriceAggregatorService {

    private static final List<String> DEFAULT_SYMBOLS = List.of("BTCUSDT", "ETHUSDT");
    private static final int BUFFER_SIZE = 10;
    private static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(2);

    private final List<PriceProvider> priceProviders;
    private final PriceCacheService priceCacheService;
    private final PriceReactiveRepository priceReactiveRepository;

    // Горячий multicast-канал для рассылки всем подписчикам (SSE, WS и внутренним сервисам)
    private final Sinks.Many<AggregatedPrice> priceSink =
            Sinks.many().multicast().onBackpressureBuffer(256, false);

    @PostConstruct
    public void startAggregationPipeline() {
        // Запускаем пайплайны агрегации для базовых криптовалютных пар
        for (String symbol : DEFAULT_SYMBOLS) {
            aggregateForSymbol(symbol)
                    .subscribe(
                            priceSink::tryEmitNext,
                            error -> log.error("Error in aggregation pipeline for {}: {}", symbol, error.getMessage())
                    );
        }

        // Подписчик для сохранения в Redis кэш и базу данных PostgreSQL без блокировки
        priceSink.asFlux()
                .flatMap(aggregatedPrice ->
                        priceCacheService.saveLatestPrice(aggregatedPrice)
                                .then(saveToDatabase(aggregatedPrice))
                                .onErrorResume(ex -> {
                                    log.warn("Failed to persist aggregated price {}: {}", aggregatedPrice.symbol(), ex.getMessage());
                                    return Mono.empty();
                                })
                )
                .subscribe();
    }

    /**
     * Поток всех агрегированных цен (для SSE и WebSocket)
     */
    public Flux<AggregatedPrice> getAggregatedPriceStream() {
        return priceSink.asFlux().onBackpressureLatest();
    }

    /**
     * Поток агрегированных цен для конкретного символа
     */
    public Flux<AggregatedPrice> getAggregatedPriceStream(String symbol) {
        return getAggregatedPriceStream()
                .filter(price -> price.symbol().equalsIgnoreCase(symbol));
    }

    /**
     * Основной пайплайн объединения и оконной агрегации
     */
    public Flux<AggregatedPrice> aggregateForSymbol(String symbol) {
        List<Flux<CryptocurrencyPrice>> streams = priceProviders.stream()
                .map(provider -> provider.getPriceStream(symbol))
                .toList();

        return Flux.merge(streams)
                .bufferTimeout(BUFFER_SIZE, BUFFER_TIMEOUT)
                .filter(ticks -> !ticks.isEmpty())
                .map(this::calculateAggregation)
                .onBackpressureLatest();
    }

    /**
     * Вычисление статистики: среднее, минимум, максимум
     */
    public AggregatedPrice calculateAggregation(List<CryptocurrencyPrice> ticks) {
        String symbol = ticks.getFirst().symbol();

        BigDecimal min = ticks.getFirst().price();
        BigDecimal max = ticks.getFirst().price();
        BigDecimal sum = BigDecimal.ZERO;

        for (CryptocurrencyPrice tick : ticks) {
            BigDecimal price = tick.price();
            if (price.compareTo(min) < 0) min = price;
            if (price.compareTo(max) > 0) max = price;
            sum = sum.add(price);
        }

        BigDecimal avg = sum.divide(BigDecimal.valueOf(ticks.size()), 4, RoundingMode.HALF_UP);

        return new AggregatedPrice(symbol, avg, min, max, Instant.now());
    }

    private Mono<PriceEntity> saveToDatabase(AggregatedPrice aggregatedPrice) {
        PriceEntity entity = PriceEntity.builder()
                .symbol(aggregatedPrice.symbol())
                .exchange("AGGREGATED")
                .price(aggregatedPrice.averagePrice())
                .createdAt(aggregatedPrice.timestamp())
                .build();

        return priceReactiveRepository.save(entity);
    }
}
