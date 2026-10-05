package io.github.evgeniylizogubov.cryptoservice.repository;

import io.github.evgeniylizogubov.cryptoservice.entity.PriceEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
public interface PriceReactiveRepository extends R2dbcRepository<PriceEntity, Long> {

    /**
     * Получить историю цен для указанного символа в порядке убывания даты добавления.
     */
    Flux<PriceEntity> findBySymbolOrderByCreatedAtDesc(String symbol);

    /**
     * Получить историю цен в заданном временном интервале.
     */
    Flux<PriceEntity> findBySymbolAndCreatedAtBetweenOrderByCreatedAtDesc(String symbol, Instant from, Instant to);

    /**
     * Получить последнюю сохранённую цену по символу (fallback для БД).
     */
    Mono<PriceEntity> findFirstBySymbolOrderByCreatedAtDesc(String symbol);
}
