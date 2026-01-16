package com.thom.rtstockpriceaggregator;

import com.thom.rtstockpriceaggregator.models.PriceUpdate;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StockPriceAggregatorTest {

    @Test
    void ingestDoesNotOverwriteNewerWithOlder() {
        var aggregator = new StockPriceAggregator();

        aggregator.ingest(new PriceUpdate("AAPL", 100, 2));
        aggregator.ingest(new PriceUpdate("AAPL", 90, 1));

        var latest = aggregator.getStockPrice("AAPL");
        assertNotNull(latest);
        assertEquals(2, latest.timestamp());
        assertEquals(100, latest.price());
    }

    @Test
    void concurrentIngestKeepsNewestTimestamp() throws InterruptedException {
        var aggregator = new StockPriceAggregator();

        int iterations = 1_000;
        for (int i = 0; i < iterations; i++) {
            var start = new CountDownLatch(1);
            var done = new CountDownLatch(2);

            var older = new PriceUpdate("AAPL", 90, 1);
            var newer = new PriceUpdate("AAPL", 100, 2);

            Thread t1 = new Thread(() -> {
                await(start);
                aggregator.ingest(older);
                done.countDown();
            });

            Thread t2 = new Thread(() -> {
                await(start);
                aggregator.ingest(newer);
                done.countDown();
            });

            t1.start();
            t2.start();
            start.countDown();
            done.await();

            var latest = aggregator.getStockPrice("AAPL");
            assertNotNull(latest);
            assertEquals(2, latest.timestamp());
            assertEquals(100, latest.price());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
