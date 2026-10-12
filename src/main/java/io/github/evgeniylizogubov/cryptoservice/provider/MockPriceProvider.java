package io.github.evgeniylizogubov.cryptoservice.provider;

import io.github.evgeniylizogubov.cryptoservice.model.CryptocurrencyPrice;
import io.github.evgeniylizogubov.cryptoservice.model.ExchangeName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@ConditionalOnProperty(name = "crypto.providers.mock.enabled", havingValue = "true")
public class MockPriceProvider implements PriceProvider {

    private static final Map<String, BigDecimal> BASE_PRICES = Map.of(
            "BTCUSDT", new BigDecimal("65000.00"),
            "ETHUSDT", new BigDecimal("3500.00")
    );
    private static final BigDecimal DEFAULT_BASE_PRICE = new BigDecimal("100.00");


    @Override
    public ExchangeName getExchange() {
        return ExchangeName.MOCK;
    }

    @Override
    public Flux<CryptocurrencyPrice> getPriceStream(String symbol) {
        return Flux.interval(Duration.ofMillis(500))
                .map(_ -> generateRandomPrice(symbol))
                .doOnNext(price -> log.trace("Mock tick generated: {}", price));
    }

    @Override
    public Mono<CryptocurrencyPrice> getLatestPrice(String symbol) {
        return Mono.fromSupplier(() -> generateRandomPrice(symbol));
    }

    private CryptocurrencyPrice generateRandomPrice(String symbol) {
        double delta = (ThreadLocalRandom.current().nextDouble() - 0.5) * 100.0;
        BigDecimal base = BASE_PRICES.getOrDefault(symbol.toUpperCase(), DEFAULT_BASE_PRICE);
        BigDecimal price = base.add(BigDecimal.valueOf(delta)).setScale(2, RoundingMode.HALF_UP);
        return new CryptocurrencyPrice(symbol.toUpperCase(), price, getExchange(), Instant.now());
    }
}
