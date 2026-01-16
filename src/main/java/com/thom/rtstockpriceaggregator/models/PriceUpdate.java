package com.thom.rtstockpriceaggregator.models;

public record PriceUpdate(String symbol, long price, long timestamp) {
}
