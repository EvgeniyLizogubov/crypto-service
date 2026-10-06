package io.github.evgeniylizogubov.cryptoservice.provider;

import io.github.evgeniylizogubov.cryptoservice.model.CryptocurrencyPrice;
import io.github.evgeniylizogubov.cryptoservice.model.ExchangeName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PriceProvider {

    /**
     * Поставщик/биржа
     */
    ExchangeName getExchange();

    /**
     * Непрерывный реактивный поток цен по указанному символу
     */
    Flux<CryptocurrencyPrice> getPriceStream(String symbol);

    /**
     * Однократное получение текущей цены (используется для fallback)
     */
    Mono<CryptocurrencyPrice> getLatestPrice(String symbol);
}
