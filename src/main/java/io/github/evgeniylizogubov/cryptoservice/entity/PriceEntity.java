package io.github.evgeniylizogubov.cryptoservice.entity;

import lombok.Builder;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Table("prices")
@Builder
public record PriceEntity(

        @Id
        Long id,

        @Column("symbol")
        String symbol,

        @Column("exchange")
        String exchange,

        @Column("price")
        BigDecimal price,

        @Column("created_at")
        Instant createdAt
) {

        public static PriceEntity newInstance(String symbol, String exchange, BigDecimal price) {
                return new PriceEntity(null, symbol, exchange, price, Instant.now());
        }
}
