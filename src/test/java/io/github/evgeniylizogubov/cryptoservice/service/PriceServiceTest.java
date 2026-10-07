package io.github.evgeniylizogubov.cryptoservice.service;

import io.github.evgeniylizogubov.cryptoservice.entity.PriceEntity;
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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PriceServiceTest {

    @Mock
    private PriceCacheService priceCacheService;

    @Mock
    private PriceReactiveRepository priceReactiveRepository;

    @Mock
    private PriceAggregatorService priceAggregatorService;

    @Mock
    private PriceProvider priceProvider;

    private PriceService priceService;

    @BeforeEach
    void setUp() {
        priceService = new PriceService(
                priceCacheService,
                priceReactiveRepository,
                List.of(priceProvider),
                priceAggregatorService
        );
    }

    @Test
    void testGetLatestPrice_CacheHit() {
        Instant now = Instant.now();
        AggregatedPrice cachedPrice = new AggregatedPrice(
                "BTCUSDT",
                new BigDecimal("60000.00"),
                new BigDecimal("59000.00"),
                new BigDecimal("61000.00"),
                now
        );

        when(priceCacheService.getLatestPrice("BTCUSDT")).thenReturn(Mono.just(cachedPrice));
        when(priceCacheService.saveLatestPrice(any())).thenReturn(Mono.just(true));

        StepVerifier.create(priceService.getLatestPrice("BTCUSDT"))
                .expectNext(cachedPrice)
                .verifyComplete();
    }

    @Test
    void testGetLatestPrice_DatabaseFallback() {
        Instant now = Instant.now();
        PriceEntity dbEntity = PriceEntity.builder()
                .id(1L)
                .symbol("BTCUSDT")
                .exchange("AGGREGATED")
                .price(new BigDecimal("60000.00"))
                .createdAt(now)
                .build();

        when(priceCacheService.getLatestPrice("BTCUSDT")).thenReturn(Mono.empty());
        when(priceReactiveRepository.findFirstBySymbolOrderByCreatedAtDesc("BTCUSDT")).thenReturn(Mono.just(dbEntity));
        when(priceCacheService.saveLatestPrice(any())).thenReturn(Mono.just(true));

        StepVerifier.create(priceService.getLatestPrice("BTCUSDT"))
                .assertNext(price -> {
                    assertEquals("BTCUSDT", price.symbol());
                    assertEquals(new BigDecimal("60000.00"), price.averagePrice());
                })
                .verifyComplete();
    }

    @Test
    void testGetLatestPrice_ProviderFallback() {
        Instant now = Instant.now();
        CryptocurrencyPrice cryptoPrice = new CryptocurrencyPrice(
                "BTCUSDT",
                new BigDecimal("60000.00"),
                ExchangeName.BINANCE,
                now
        );

        when(priceCacheService.getLatestPrice("BTCUSDT")).thenReturn(Mono.empty());
        when(priceReactiveRepository.findFirstBySymbolOrderByCreatedAtDesc("BTCUSDT")).thenReturn(Mono.empty());
        when(priceProvider.getLatestPrice("BTCUSDT")).thenReturn(Mono.just(cryptoPrice));
        when(priceCacheService.saveLatestPrice(any())).thenReturn(Mono.just(true));

        StepVerifier.create(priceService.getLatestPrice("BTCUSDT"))
                .assertNext(price -> {
                    assertEquals("BTCUSDT", price.symbol());
                    assertEquals(new BigDecimal("60000.00"), price.averagePrice());
                })
                .verifyComplete();
    }

    @Test
    void testGetPriceHistory() {
        Instant now = Instant.now();
        PriceEntity entity = PriceEntity.builder()
                .id(1L)
                .symbol("BTCUSDT")
                .exchange("BINANCE")
                .price(new BigDecimal("60000.00"))
                .createdAt(now)
                .build();

        when(priceReactiveRepository.findBySymbolOrderByCreatedAtDesc("BTCUSDT"))
                .thenReturn(Flux.just(entity));

        StepVerifier.create(priceService.getPriceHistory("BTCUSDT"))
                .expectNext(entity)
                .verifyComplete();
    }

    @Test
    void testGetPriceStream() {
        Instant now = Instant.now();
        AggregatedPrice price = new AggregatedPrice(
                "BTCUSDT",
                new BigDecimal("60000.00"),
                new BigDecimal("59000.00"),
                new BigDecimal("61000.00"),
                now
        );

        when(priceAggregatorService.getAggregatedPriceStream("BTCUSDT"))
                .thenReturn(Flux.just(price));

        StepVerifier.create(priceService.getPriceStream("BTCUSDT"))
                .expectNext(price)
                .verifyComplete();
    }
}
