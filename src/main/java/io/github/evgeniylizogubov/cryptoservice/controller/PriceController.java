package io.github.evgeniylizogubov.cryptoservice.controller;

import io.github.evgeniylizogubov.cryptoservice.entity.PriceEntity;
import io.github.evgeniylizogubov.cryptoservice.model.AggregatedPrice;
import io.github.evgeniylizogubov.cryptoservice.service.PriceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/prices")
@RequiredArgsConstructor
public class PriceController {

    private final PriceService priceService;

    @GetMapping("/{symbol}")
    public Mono<ResponseEntity<AggregatedPrice>> getLatestPrice(@PathVariable String symbol) {
        return priceService.getLatestPrice(symbol)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/{symbol}/history")
    public Flux<PriceEntity> getPriceHistory(@PathVariable String symbol) {
        return priceService.getPriceHistory(symbol);
    }

    @GetMapping(value = "/{symbol}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<AggregatedPrice> getPriceStream(@PathVariable String symbol) {
        return priceService.getPriceStream(symbol);
    }
}
