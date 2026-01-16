# rt-stock-price-aggregator

Random thread safe price aggregator - might be cool to wire into some live data
feed

## Run tests

```bash
./mvnw test
```

## Run the stress runner (from `main`)

`App` runs a concurrent ingest + query stress workload.

```bash
./mvnw -q exec:java
```

Optional args:

```bash
./mvnw -q exec:java -Dexec.args="--symbols=2000 --updatesPerSymbol=200 --updateThreads=16 --queryThreads=8 --seed=123"
```
