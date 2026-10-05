package io.github.evgeniylizogubov.cryptoservice.config;

import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.model.CryptocurrencyPrice;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public ReactiveRedisTemplate<String, AggregatedPrice> aggregatedPriceRedisTemplate(
            ReactiveRedisConnectionFactory factory
    ) {
        JacksonJsonRedisSerializer<AggregatedPrice> serializer =
                new JacksonJsonRedisSerializer<>(AggregatedPrice.class);

        RedisSerializationContext<String, AggregatedPrice> context = RedisSerializationContext
                .<String, AggregatedPrice>newSerializationContext(RedisSerializer.string())
                .value(serializer)
                .build();

        return new ReactiveRedisTemplate<>(factory, context);
    }

    @Bean
    public ReactiveRedisTemplate<String, CryptocurrencyPrice> cryptocurrencyPriceRedisTemplate(
            ReactiveRedisConnectionFactory factory
    ) {
        JacksonJsonRedisSerializer<CryptocurrencyPrice> serializer =
                new JacksonJsonRedisSerializer<>(CryptocurrencyPrice.class);

        RedisSerializationContext<String, CryptocurrencyPrice> context = RedisSerializationContext
                .<String, CryptocurrencyPrice>newSerializationContext(RedisSerializer.string())
                .value(serializer)
                .build();

        return new ReactiveRedisTemplate<>(factory, context);
    }
}
