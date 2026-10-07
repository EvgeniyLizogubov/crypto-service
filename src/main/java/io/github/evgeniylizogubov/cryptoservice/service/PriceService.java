package io.github.evgeniylizogubov.cryptoservice.service;

import io.github.evgeniylizogubov.cryptoservice.entity.PriceEntity;
import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.provider.PriceProvider;
import io.github.evgeniylizogubov.cryptoservice.repository.PriceReactiveRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PriceService {

    private final PriceCacheService priceCacheService;
    private final PriceReactiveRepository priceReactiveRepository;
    private final List<PriceProvider> priceProviders;
    private final PriceAggregatorService priceAggregatorService;

    /**
     * Каскадный поиск цены: Redis Cache -> PostgreSQL R2DBC -> External Provider Fallback
     */
    public Mono<AggregatedPrice> getLatestPrice(String symbol) {
        String normalizedSymbol = symbol.toUpperCase();

        return priceCacheService.getLatestPrice(normalizedSymbol)
                .switchIfEmpty(Mono.defer(() -> getFromDatabase(normalizedSymbol)))
                .switchIfEmpty(Mono.defer(() -> getFromExternalProvider(normalizedSymbol)))
                .delayUntil(price -> priceCacheService.saveLatestPrice(price)
                        .onErrorResume(ex -> {
                            log.error("Failed to save price to cache for {}: {}", price.symbol(), ex.getMessage());
                            return Mono.empty();
                        })
                );
    }

    /**
     * Поток истории цен из PostgreSQL R2DBC
     */
    public Flux<PriceEntity> getPriceHistory(String symbol) {
        return priceReactiveRepository.findBySymbolOrderByCreatedAtDesc(symbol.toUpperCase());
    }

    /**
     * SSE поток цен для конкретного символа
     */
    public Flux<AggregatedPrice> getPriceStream(String symbol) {
        return priceAggregatorService.getAggregatedPriceStream(symbol);
    }

    private Mono<AggregatedPrice> getFromDatabase(String symbol) {
        return priceReactiveRepository.findFirstBySymbolOrderByCreatedAtDesc(symbol)
                .map(entity -> new AggregatedPrice(
                        entity.symbol(),
                        entity.price(),
                        entity.price(),
                        entity.price(),
                        entity.createdAt()
                ))
                .doOnNext(_ -> log.debug("Fallback to Database hit for symbol: {}", symbol));
    }

    private Mono<AggregatedPrice> getFromExternalProvider(String symbol) {
        if (priceProviders.isEmpty()) {
            return Mono.empty();
        }

        // Опрашиваем первый ответивший провайдер
        return Flux.fromIterable(priceProviders)
                .flatMap(provider -> provider.getLatestPrice(symbol).onErrorResume(_ -> Mono.empty()))
                .next()
                .map(cryptoPrice -> new AggregatedPrice(
                        cryptoPrice.symbol(),
                        cryptoPrice.price(),
                        cryptoPrice.price(),
                        cryptoPrice.price(),
                        cryptoPrice.timestamp()
                ))
                .doOnNext(_ -> log.debug("Fallback to External Provider hit for symbol: {}", symbol));
    }
}
