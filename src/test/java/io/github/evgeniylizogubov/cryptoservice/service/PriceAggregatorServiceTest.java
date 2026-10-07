package io.github.evgeniylizogubov.cryptoservice.service;

import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.model.CryptocurrencyPrice;
import io.github.evgeniylizogubov.cryptoservice.model.ExchangeName;
import io.github.evgeniylizogubov.cryptoservice.provider.PriceProvider;
import io.github.evgeniylizogubov.cryptoservice.repository.PriceReactiveRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PriceAggregatorServiceTest {

    @Mock
    private PriceCacheService priceCacheService;

    @Mock
    private PriceReactiveRepository priceReactiveRepository;

    @Mock
    private PriceProvider provider1;

    @Mock
    private PriceProvider provider2;

    private PriceAggregatorService service;

    @BeforeEach
    void setUp() {
        service = new PriceAggregatorService(
                List.of(provider1, provider2),
                priceCacheService,
                priceReactiveRepository
        );
    }

    @Test
    void testCalculateAggregation() {
        Instant now = Instant.now();
        List<CryptocurrencyPrice> ticks = List.of(
                new CryptocurrencyPrice("BTCUSDT", new BigDecimal("100.00"), ExchangeName.BINANCE, now),
                new CryptocurrencyPrice("BTCUSDT", new BigDecimal("110.00"), ExchangeName.COINBASE, now),
                new CryptocurrencyPrice("BTCUSDT", new BigDecimal("90.00"), ExchangeName.KRAKEN, now)
        );

        AggregatedPrice aggregated = service.calculateAggregation(ticks);

        assertEquals("BTCUSDT", aggregated.symbol());
        assertEquals(new BigDecimal("90.00"), aggregated.minPrice());
        assertEquals(new BigDecimal("110.00"), aggregated.maxPrice());
        assertEquals(new BigDecimal("100.0000"), aggregated.averagePrice());
    }

    @Test
    void testAggregateForSymbol() {
        Instant now = Instant.now();
        Flux<CryptocurrencyPrice> stream1 = Flux.just(
                new CryptocurrencyPrice("BTCUSDT", new BigDecimal("100.00"), ExchangeName.BINANCE, now)
        );
        Flux<CryptocurrencyPrice> stream2 = Flux.just(
                new CryptocurrencyPrice("BTCUSDT", new BigDecimal("200.00"), ExchangeName.COINBASE, now)
        );

        when(provider1.getPriceStream("BTCUSDT")).thenReturn(stream1);
        when(provider2.getPriceStream("BTCUSDT")).thenReturn(stream2);

        StepVerifier.create(service.aggregateForSymbol("BTCUSDT"))
                .assertNext(aggregatedPrice -> {
                    assertEquals("BTCUSDT", aggregatedPrice.symbol());
                    assertEquals(new BigDecimal("100.00"), aggregatedPrice.minPrice());
                    assertEquals(new BigDecimal("200.00"), aggregatedPrice.maxPrice());
                    assertEquals(new BigDecimal("150.0000"), aggregatedPrice.averagePrice());
                })
                .verifyComplete();
    }
}
