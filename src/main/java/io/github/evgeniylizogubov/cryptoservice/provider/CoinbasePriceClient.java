package io.github.evgeniylizogubov.cryptoservice.provider;

import io.github.evgeniylizogubov.cryptoservice.model.CryptocurrencyPrice;
import io.github.evgeniylizogubov.cryptoservice.model.ExchangeName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
public class CoinbasePriceClient implements PriceProvider {

    private final WebClient webClient;
    private final Duration pollInterval;

    public CoinbasePriceClient(
            WebClient.Builder webClientBuilder,
            @Value("${crypto.providers.coinbase.base-url}") String baseUrl,
            @Value("${crypto.providers.coinbase.poll-interval}") Duration pollInterval
    ) {
        this.webClient = webClientBuilder.baseUrl(baseUrl).build();
        this.pollInterval = pollInterval;
    }

    @Override
    public ExchangeName getExchange() {
        return ExchangeName.COINBASE;
    }

    @Override
    public Flux<CryptocurrencyPrice> getPriceStream(String symbol) {
        return Flux.interval(pollInterval)
                .flatMap(_ -> getLatestPrice(symbol)
                        .onErrorResume(ex -> {
                            log.warn("Error polling Coinbase price for {}: {}", symbol, ex.getMessage());
                            return Mono.empty();
                        })
                );
    }

    @Override
    public Mono<CryptocurrencyPrice> getLatestPrice(String symbol) {
        String formattedSymbol = formatSymbol(symbol);
        return webClient.get()
                .uri("/products/{product_id}/ticker", formattedSymbol)
                .retrieve()
                .bodyToMono(CoinbaseTickerResponse.class)
                .timeout(Duration.ofSeconds(3))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(300)).maxBackoff(Duration.ofSeconds(2)))
                .map(response -> new CryptocurrencyPrice(
                        symbol.toUpperCase(),
                        response.price(),
                        ExchangeName.COINBASE,
                        response.time() != null ? response.time() : Instant.now()
                ));

    }

    private String formatSymbol(String symbol) {
        return symbol.replace("USDT", "-USDT");
    }

    public record CoinbaseTickerResponse(BigDecimal price, Instant time) {}
}
