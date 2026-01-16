package com.thom.rtstockpriceaggregator;

import com.thom.rtstockpriceaggregator.models.PriceUpdate;
import com.thom.rtstockpriceaggregator.models.StockPrice;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class StockPriceAggregator {

    private final ConcurrentHashMap<String, StockPrice> stockPricesMap = new ConcurrentHashMap<>();

    public void ingest(PriceUpdate update) {
        Objects.requireNonNull(update, "update");

        var symbol = Objects.requireNonNull(update.symbol(), "symbol");
        var candidate = new StockPrice(update.price(), update.timestamp());

        stockPricesMap.compute(symbol, (k, existing) -> {
            if (existing == null) {
                return candidate;
            }
            if (candidate.timestamp() > existing.timestamp()) {
                return candidate;
            }
            return existing;
        });
    }

    public StockPrice getStockPrice(String symbol) {
        return stockPricesMap.get(symbol);
    }
}
