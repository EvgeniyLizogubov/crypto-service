package io.github.evgeniylizogubov.cryptoservice.model;

import java.math.BigDecimal;
import java.time.Instant;

public record CryptocurrencyPrice(
        String symbol,
        BigDecimal price,
        ExchangeName exchange,
        Instant timestamp
) {
}
