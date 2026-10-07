package io.github.evgeniylizogubov.cryptoservice.websocket;

import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.service.PriceAggregatorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
@NullMarked
public class CryptoPriceWebSocketHandler implements WebSocketHandler {

    private final PriceAggregatorService priceAggregatorService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        // Множество подписанных символов для текущей сессии
        Set<String> subscribedSymbols = Collections.newSetFromMap(new ConcurrentHashMap<>());

        // Обработка входящих сообщений подписки (например, "BTCUSDT" или "BTCUSDT,ETHUSDT")
        Mono<Void> input = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> {
                    log.info("WebSocket subscription received: {}", payload);
                    String[] symbols = payload.replaceAll("[\"' ]", "").toUpperCase().split(",");
                    for (String s : symbols) {
                        if (!s.isBlank()) {
                            subscribedSymbols.add(s);
                        }
                    }
                })
                .doOnError(ex -> log.error("WebSocket receive error: {}", ex.getMessage()))
                .then();

        Flux<WebSocketMessage> output = priceAggregatorService.getAggregatedPriceStream()
                .filter(price -> subscribedSymbols.isEmpty() || subscribedSymbols.contains(price.symbol()))
                .map(this::toJson)
                .map(session::textMessage);

        return Mono.when(input, session.send(output)).then();
    }

    private String toJson(AggregatedPrice price) {
        try {
            return objectMapper.writeValueAsString(price);
        } catch (Exception e) {
            log.error("Failed to serialize AggregatedPrice to JSON", e);
            return "{}";
        }
    }
}
