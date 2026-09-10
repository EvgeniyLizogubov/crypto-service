package io.github.evgeniylizogubov.cryptoservice.model;

import java.math.BigDecimal;
import java.time.Instant;

public record AggregatedPrice(
        String symbol,
        BigDecimal averagePrice,
        BigDecimal minPrice,
        BigDecimal maxPrice,
        Instant timestamp
) {
}
