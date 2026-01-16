package com.thom.rtstockpriceaggregator;

import com.thom.rtstockpriceaggregator.models.PriceUpdate;

public class App {

    public static void main(String[] args) {
        var aggregator = new StockPriceAggregator();

        aggregator.ingest(new PriceUpdate("AAPL", 100, 1));
        aggregator.ingest(new PriceUpdate("AAPL", 90, 0));
        aggregator.ingest(new PriceUpdate("AAPL", 110, 2));

        System.out.println("AAPL latest: " + aggregator.getStockPrice("AAPL"));
        System.out.println("MSFT latest: " + aggregator.getStockPrice("MSFT"));
    }
}
