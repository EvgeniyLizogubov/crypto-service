package io.github.evgeniylizogubov.cryptoservice.config;

import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RedisConfigTest {

    @Test
    void testAggregatedPriceSerializationAndDeserialization() {
        JacksonJsonRedisSerializer<AggregatedPrice> serializer =
                new JacksonJsonRedisSerializer<>(AggregatedPrice.class);

        Instant now = Instant.parse("2026-10-05T12:00:00Z");
        AggregatedPrice price = new AggregatedPrice(
                "BTCUSDT",
                new BigDecimal("65000.50"),
                new BigDecimal("64500.00"),
                new BigDecimal("65500.00"),
                now
        );

        byte[] bytes = serializer.serialize(price);
        assertNotNull(bytes);

        AggregatedPrice deserialized = serializer.deserialize(bytes);
        assertNotNull(deserialized);
        assertEquals(price.symbol(), deserialized.symbol());
        assertEquals(price.averagePrice(), deserialized.averagePrice());
        assertEquals(price.minPrice(), deserialized.minPrice());
        assertEquals(price.maxPrice(), deserialized.maxPrice());
        assertEquals(price.timestamp(), deserialized.timestamp());
    }
}
