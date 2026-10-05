package io.github.evgeniylizogubov.cryptoservice.service;

import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class PriceCacheService {

    private static final String KEY_PREFIX = "crypto:";
    private static final String KEY_SUFFIX = ":last_price";
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(5);

    private final ReactiveRedisTemplate<String, AggregatedPrice> aggregatedPriceRedisTemplate;

    public Mono<AggregatedPrice> getLatestPrice(String symbol) {
        String key = buildKey(symbol);
        return aggregatedPriceRedisTemplate.opsForValue()
                .get(key)
                .doOnNext(price -> log.debug("Cache HIT for symbol: {}", symbol))
                .onErrorResume(ex -> Mono.empty());
    }

    public Mono<Boolean> saveLatestPrice(AggregatedPrice price) {
        if (price == null || price.symbol() == null) {
            return Mono.just(false);
        }

        String key = buildKey(price.symbol());
        return aggregatedPriceRedisTemplate.opsForValue()
                .set(key, price, DEFAULT_TTL)
                .doOnError(ex -> log.error("Error saving to Redis for key {}: {}", key, ex.getMessage()))
                .onErrorReturn(false);
    }

    private String buildKey(String symbol) {
        return KEY_PREFIX + symbol.toUpperCase() + KEY_SUFFIX;
    }
}
