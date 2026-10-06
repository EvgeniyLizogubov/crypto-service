package io.github.evgeniylizogubov.cryptoservice.provider;

import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class KrakenPriceClient implements PriceProvider {

    private final WebClient webClient;
    private final Duration pollInterval;

    public KrakenPriceClient(
            WebClient.Builder webClientBuilder,
            @Value("${crypto.providers.kraken.base-url}") String baseUrl,
            @Value("${crypto.providers.kraken.poll-interval}") Duration pollInterval
    ) {
        this.webClient = webClientBuilder.baseUrl(baseUrl).build();
        this.pollInterval = pollInterval;
    }

    @Override
    public ExchangeName getExchange() {
        return ExchangeName.KRAKEN;
    }

    @Override
    public Flux<CryptocurrencyPrice> getPriceStream(String symbol) {
        return Flux.interval(pollInterval)
                .flatMap(_ -> getLatestPrice(symbol)
                        .onErrorResume(ex -> {
                            log.warn("Error polling Kraken price for {}: {}", symbol, ex.getMessage());
                            return Mono.empty();
                        })
                );
    }

    @Override
    public Mono<CryptocurrencyPrice> getLatestPrice(String symbol) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/0/public/Ticker")
                        .queryParam("pair", symbol.toUpperCase())
                        .build())
                .retrieve()
                .bodyToMono(KrakenTickerResponse.class)
                .timeout(Duration.ofSeconds(3))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(300)).maxBackoff(Duration.ofSeconds(2)))
                .flatMap(response -> {
                            if (response.error() != null && !response.error().isEmpty()) {
                                log.warn("Kraken API returned errors: {}", response.error());
                                return Mono.empty();
                            }

                            if (response.result() == null || response.result().isEmpty()) {
                                return Mono.empty();
                            }

                            KrakenTickerResponse.KrakenPairInfo pairInfo = response.result().values().iterator().next();
                            return Mono.just(new CryptocurrencyPrice(
                                    symbol.toUpperCase(),
                                    pairInfo.getPrice(),
                                    ExchangeName.KRAKEN,
                                    Instant.now()
                            ));
                });

    }

    public record KrakenTickerResponse(List<String> error, Map<String, KrakenPairInfo> result) {
        public record KrakenPairInfo(@JsonProperty("c") List<String> lastTradeClosed) {
            public BigDecimal getPrice() {
                if (lastTradeClosed == null || lastTradeClosed.isEmpty()) {
                    return BigDecimal.ZERO;
                }
                return new BigDecimal(lastTradeClosed.getFirst());
            }
        }
    }
}
