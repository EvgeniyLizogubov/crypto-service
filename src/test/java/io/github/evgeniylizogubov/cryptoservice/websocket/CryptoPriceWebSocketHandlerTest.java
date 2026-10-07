package io.github.evgeniylizogubov.cryptoservice.websocket;

import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.service.PriceAggregatorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CryptoPriceWebSocketHandlerTest {

    @Mock
    private PriceAggregatorService priceAggregatorService;

    @Mock
    private WebSocketSession session;

    private CryptoPriceWebSocketHandler handler;

    @BeforeEach
    void setUp() {
        handler = new CryptoPriceWebSocketHandler(priceAggregatorService);
    }

    @Test
    void testHandle_FiltersBySubscription() {
        WebSocketMessage message = mock(WebSocketMessage.class);
        when(message.getPayloadAsText()).thenReturn("BTCUSDT");
        when(session.receive()).thenReturn(Flux.just(message));

        AggregatedPrice btcPrice = new AggregatedPrice(
                "BTCUSDT",
                new BigDecimal("60000.00"),
                new BigDecimal("59000.00"),
                new BigDecimal("61000.00"),
                Instant.now()
        );
        AggregatedPrice ethPrice = new AggregatedPrice(
                "ETHUSDT",
                new BigDecimal("3000.00"),
                new BigDecimal("2900.00"),
                new BigDecimal("3100.00"),
                Instant.now()
        );

        when(priceAggregatorService.getAggregatedPriceStream()).thenReturn(Flux.just(btcPrice, ethPrice));
        lenient().when(session.textMessage(any(String.class))).thenAnswer(_ -> mock(WebSocketMessage.class));
        when(session.send(any())).thenAnswer(invocation -> {
            Publisher<?> publisher = invocation.getArgument(0);
            return Flux.from(publisher).then();
        });

        StepVerifier.create(handler.handle(session))
                .verifyComplete();
    }
}
