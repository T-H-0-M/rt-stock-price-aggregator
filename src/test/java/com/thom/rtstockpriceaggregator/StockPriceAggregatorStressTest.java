package com.thom.rtstockpriceaggregator;

import com.thom.rtstockpriceaggregator.models.PriceUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StockPriceAggregatorStressTest {

    @Test
    @Timeout(10)
    void concurrentIngestAndQueryNeverRegressesAndEndsCorrect() throws Exception {
        int symbolCount = 500;
        int updatesPerSymbol = 50;

        int updateThreads = 8;
        int queryThreads = 4;

        String[] symbols = new String[symbolCount];
        for (int i = 0; i < symbolCount; i++) {
            symbols[i] = "SYM" + i;
        }

        List<PriceUpdate> updates = new ArrayList<>(symbolCount * updatesPerSymbol);
        long[] expectedMaxTimestamp = new long[symbolCount];
        Arrays.fill(expectedMaxTimestamp, -1);

        for (int symbolIndex = 0; symbolIndex < symbolCount; symbolIndex++) {
            for (long ts = 0; ts < updatesPerSymbol; ts++) {
                long price = symbolIndex * 1_000_000L + ts;
                updates.add(new PriceUpdate(symbols[symbolIndex], price, ts));
                expectedMaxTimestamp[symbolIndex] = Math.max(expectedMaxTimestamp[symbolIndex], ts);
            }
        }

        Collections.shuffle(updates, new Random(123));

        List<List<PriceUpdate>> slices = new ArrayList<>(updateThreads);
        for (int i = 0; i < updateThreads; i++) {
            slices.add(new ArrayList<>());
        }
        for (int i = 0; i < updates.size(); i++) {
            slices.get(i % updateThreads).add(updates.get(i));
        }

        var aggregator = new StockPriceAggregator();

        var start = new CountDownLatch(1);
        var running = new AtomicBoolean(true);
        var failure = new AtomicReference<Throwable>();

        ExecutorService exec = Executors.newFixedThreadPool(updateThreads + queryThreads);
        try {
            var queryDone = new CountDownLatch(queryThreads);
            for (int i = 0; i < queryThreads; i++) {
                int threadIndex = i;
                exec.submit(() -> {
                    long[] lastSeenTimestamp = new long[symbolCount];
                    Arrays.fill(lastSeenTimestamp, -1);

                    Random rnd = new Random(1_000 + threadIndex);
                    await(start);

                    while (running.get() && failure.get() == null) {
                        int symbolIndex = rnd.nextInt(symbolCount);
                        var symbol = symbols[symbolIndex];

                        var price = aggregator.getStockPrice(symbol);
                        if (price == null) {
                            continue;
                        }

                        long seenTs = price.timestamp();
                        long prevTs = lastSeenTimestamp[symbolIndex];
                        if (seenTs < prevTs) {
                            failure.compareAndSet(null, new AssertionError(
                                    "Timestamp regressed for " + symbol + ": saw " + seenTs + " after " + prevTs));
                            break;
                        }

                        lastSeenTimestamp[symbolIndex] = seenTs;
                    }

                    queryDone.countDown();
                });
            }

            List<Future<?>> updateFutures = new ArrayList<>(updateThreads);
            for (var slice : slices) {
                updateFutures.add(exec.submit(() -> {
                    await(start);
                    for (var update : slice) {
                        if (failure.get() != null) {
                            break;
                        }
                        aggregator.ingest(update);
                    }
                }));
            }

            start.countDown();

            for (var f : updateFutures) {
                f.get();
            }

            running.set(false);
            queryDone.await();

            Throwable t = failure.get();
            if (t != null) {
                if (t instanceof Exception e) {
                    throw e;
                }
                if (t instanceof Error e) {
                    throw e;
                }
                throw new RuntimeException(t);
            }
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(1, TimeUnit.SECONDS);
        }

        for (int symbolIndex = 0; symbolIndex < symbolCount; symbolIndex++) {
            var symbol = symbols[symbolIndex];
            var latest = aggregator.getStockPrice(symbol);

            assertNotNull(latest, "Missing final price for " + symbol);
            assertEquals(expectedMaxTimestamp[symbolIndex], latest.timestamp(), "Wrong final timestamp for " + symbol);

            long expectedPrice = symbolIndex * 1_000_000L + expectedMaxTimestamp[symbolIndex];
            assertEquals(expectedPrice, latest.price(), "Wrong final price for " + symbol);
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
