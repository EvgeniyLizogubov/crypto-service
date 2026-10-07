package io.github.evgeniylizogubov.cryptoservice.controller;

import io.github.evgeniylizogubov.cryptoservice.entity.PriceEntity;
import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.service.PriceService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PriceControllerTest {

    @Mock
    private PriceService priceService;

    @InjectMocks
    private PriceController priceController;

    @Test
    void testGetLatestPrice_Found() {
        AggregatedPrice price = new AggregatedPrice(
                "BTCUSDT",
                new BigDecimal("60000.00"),
                new BigDecimal("59000.00"),
                new BigDecimal("61000.00"),
                Instant.now()
        );

        when(priceService.getLatestPrice("BTCUSDT")).thenReturn(Mono.just(price));

        StepVerifier.create(priceController.getLatestPrice("BTCUSDT"))
                .assertNext(response -> {
                    assertEquals(HttpStatus.OK, response.getStatusCode());
                    assertEquals(price, response.getBody());
                })
                .verifyComplete();
    }

    @Test
    void testGetLatestPrice_NotFound() {
        when(priceService.getLatestPrice("BTCUSDT")).thenReturn(Mono.empty());

        StepVerifier.create(priceController.getLatestPrice("BTCUSDT"))
                .assertNext(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode()))
                .verifyComplete();
    }

    @Test
    void testGetPriceHistory() {
        PriceEntity entity = PriceEntity.builder()
                .id(1L)
                .symbol("BTCUSDT")
                .exchange("BINANCE")
                .price(new BigDecimal("60000.00"))
                .createdAt(Instant.now())
                .build();

        when(priceService.getPriceHistory("BTCUSDT")).thenReturn(Flux.just(entity));

        StepVerifier.create(priceController.getPriceHistory("BTCUSDT"))
                .expectNext(entity)
                .verifyComplete();
    }

    @Test
    void testGetPriceStream() {
        AggregatedPrice price = new AggregatedPrice(
                "BTCUSDT",
                new BigDecimal("60000.00"),
                new BigDecimal("59000.00"),
                new BigDecimal("61000.00"),
                Instant.now()
        );

        when(priceService.getPriceStream("BTCUSDT")).thenReturn(Flux.just(price));

        StepVerifier.create(priceController.getPriceStream("BTCUSDT"))
                .expectNext(price)
                .verifyComplete();
    }
}
