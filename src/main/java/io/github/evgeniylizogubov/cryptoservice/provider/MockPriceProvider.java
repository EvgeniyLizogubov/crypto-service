package io.github.evgeniylizogubov.cryptoservice.provider;

import io.github.evgeniylizogubov.cryptoservice.model.CryptocurrencyPrice;
import io.github.evgeniylizogubov.cryptoservice.model.ExchangeName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
public class MockPriceProvider implements PriceProvider {

    private static final BigDecimal BASE_PRICE = new BigDecimal("65000.00");


    @Override
    public ExchangeName getExchange() {
        return ExchangeName.BINANCE;
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
        BigDecimal price = BASE_PRICE.add(BigDecimal.valueOf(delta)).setScale(2, RoundingMode.HALF_UP);
        return new CryptocurrencyPrice(symbol.toUpperCase(), price, getExchange(), Instant.now());
    }
}
