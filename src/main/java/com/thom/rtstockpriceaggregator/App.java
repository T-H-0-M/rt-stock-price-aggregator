package com.thom.rtstockpriceaggregator;

import com.thom.rtstockpriceaggregator.models.PriceUpdate;

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

/**
 * Local stress runner: concurrent ingest + concurrent reads.
 *
 * Args (optional):
 * - --symbols=500
 * - --updatesPerSymbol=50
 * - --updateThreads=8
 * - --queryThreads=4
 * - --seed=123
 */
public class App {

    public static void main(String[] args) throws Exception {
        int symbolCount = intArg(args, "--symbols", 500);
        int updatesPerSymbol = intArg(args, "--updatesPerSymbol", 50);
        int updateThreads = intArg(args, "--updateThreads", 8);
        int queryThreads = intArg(args, "--queryThreads", 4);
        long seed = longArg(args, "--seed", 123L);
        boolean verbose = boolArg(args, "--verbose", true);

        runStress(symbolCount, updatesPerSymbol, updateThreads, queryThreads, seed, verbose);
    }

    private static void runStress(
            int symbolCount,
            int updatesPerSymbol,
            int updateThreads,
            int queryThreads,
            long seed,
            boolean verbose
    ) throws Exception {
        var aggregator = new StockPriceAggregator();

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

        Collections.shuffle(updates, new Random(seed));

        List<List<PriceUpdate>> slices = new ArrayList<>(updateThreads);
        for (int i = 0; i < updateThreads; i++) {
            slices.add(new ArrayList<>());
        }
        for (int i = 0; i < updates.size(); i++) {
            slices.get(i % updateThreads).add(updates.get(i));
        }

        var start = new CountDownLatch(1);
        var running = new AtomicBoolean(true);
        var failure = new AtomicReference<Throwable>();

        int totalThreads = updateThreads + queryThreads;
        ExecutorService exec = Executors.newFixedThreadPool(totalThreads);

        long beginNanos = System.nanoTime();
        try {
            var queryDone = new CountDownLatch(queryThreads);
            for (int i = 0; i < queryThreads; i++) {
                int threadIndex = i;
                exec.submit(() -> {
                    Thread.currentThread().setName("query-" + threadIndex);

                    long[] lastSeenTimestamp = new long[symbolCount];
                    Arrays.fill(lastSeenTimestamp, -1);

                    Random rnd = new Random(seed + 1_000 + threadIndex);
                    long reads = 0;
                    long logEveryReads = 200_000;

                    log("starting query loop");
                    await(start);

                    while (running.get() && failure.get() == null) {
                        int symbolIndex = rnd.nextInt(symbolCount);
                        var symbol = symbols[symbolIndex];

                        var latest = aggregator.getStockPrice(symbol);
                        reads++;

                        if (latest != null) {
                            long seenTs = latest.timestamp();
                            long prevTs = lastSeenTimestamp[symbolIndex];
                            if (seenTs < prevTs) {
                                failure.compareAndSet(null, new AssertionError(
                                        "Timestamp regressed for " + symbol + ": saw " + seenTs + " after " + prevTs));
                                break;
                            }

                            lastSeenTimestamp[symbolIndex] = seenTs;
                        }

                        if (verbose && reads % logEveryReads == 0) {
                            log("reads=" + reads);
                        }
                    }

                    log("stopping; reads=" + reads);
                    queryDone.countDown();
                });
            }

            List<Future<?>> updateFutures = new ArrayList<>(updateThreads);
            for (int i = 0; i < slices.size(); i++) {
                int threadIndex = i;
                var slice = slices.get(i);

                updateFutures.add(exec.submit(() -> {
                    Thread.currentThread().setName("update-" + threadIndex);

                    int total = slice.size();
                    int processed = 0;
                    int progressStep = Math.max(1, total / 5);

                    log("starting ingest; updates=" + total);
                    await(start);

                    for (var update : slice) {
                        if (failure.get() != null) {
                            break;
                        }
                        aggregator.ingest(update);
                        processed++;

                        if (verbose && processed % progressStep == 0) {
                            log("ingested " + processed + "/" + total);
                        }
                    }

                    log("done ingest; processed=" + processed + "/" + total);
                }));
            }

            System.out.printf(
                    "Running stress: symbols=%d updatesPerSymbol=%d totalUpdates=%d updateThreads=%d queryThreads=%d seed=%d verbose=%s%n",
                    symbolCount,
                    updatesPerSymbol,
                    updates.size(),
                    updateThreads,
                    queryThreads,
                    seed,
                    verbose
            );

            start.countDown();

            for (var f : updateFutures) {
                f.get();
            }

            running.set(false);
            queryDone.await();

            Throwable t = failure.get();
            if (t != null) {
                log("failure captured: " + t.getMessage());
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
            if (latest == null) {
                throw new AssertionError("Missing final price for " + symbol);
            }

            long expectedTs = expectedMaxTimestamp[symbolIndex];
            if (latest.timestamp() != expectedTs) {
                throw new AssertionError(
                        "Wrong final timestamp for " + symbol + ": expected=" + expectedTs + " actual=" + latest.timestamp());
            }

            long expectedPrice = symbolIndex * 1_000_000L + expectedTs;
            if (latest.price() != expectedPrice) {
                throw new AssertionError(
                        "Wrong final price for " + symbol + ": expected=" + expectedPrice + " actual=" + latest.price());
            }
        }

        long durationNanos = System.nanoTime() - beginNanos;
        double durationSeconds = durationNanos / 1_000_000_000.0;
        double updatesPerSecond = updates.size() / durationSeconds;

        System.out.printf(
                "OK: verified %d symbols; %.3fs; ingest throughput ~ %.0f updates/sec%n",
                symbolCount,
                durationSeconds,
                updatesPerSecond
        );
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void log(String message) {
        System.out.println("Thread " + Thread.currentThread().getName() + " - " + message);
    }

    private static boolean boolArg(String[] args, String key, boolean defaultValue) {
        String raw = arg(args, key);
        if (raw == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw);
    }

    private static int intArg(String[] args, String key, int defaultValue) {
        String raw = arg(args, key);
        if (raw == null) {
            return defaultValue;
        }
        return Integer.parseInt(raw);
    }

    private static long longArg(String[] args, String key, long defaultValue) {
        String raw = arg(args, key);
        if (raw == null) {
            return defaultValue;
        }
        return Long.parseLong(raw);
    }

    private static String arg(String[] args, String key) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals(key) && i + 1 < args.length) {
                return args[i + 1];
            }
            if (arg.startsWith(key + "=")) {
                return arg.substring((key + "=").length());
            }
        }
        return null;
    }
}
