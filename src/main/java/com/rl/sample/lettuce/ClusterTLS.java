package com.rl.sample.lettuce;


import io.lettuce.core.RedisURI;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "pool", abbreviateSynopsis = true)
public class ClusterTLS implements Runnable {

    private final static Logger log = LoggerFactory.getLogger(ClusterTLS.class);
    @CommandLine.Option(names = {"--host"}, description = "Redis Server host (default: ${DEFAULT-VALUE})", defaultValue = "localhost")
    public String host;
    @CommandLine.Option(names = {"--port"}, description = "Redis Server port (default: ${DEFAULT-VALUE})", defaultValue = "6379")
    public int port;

    @CommandLine.Option(names = {"--password"}, description = "Redis Server password")
    public String password;
    RedisURI redisURI;
    @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of worker threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 5;
    @CommandLine.Option(names = {"-i", "--iterations"}, description = "Worker iterations (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int iterations = 1000;
    @CommandLine.Option(names = {"-p", "--pipeline"}, description = "#commands to run in a pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int pipeline = 50;
    @CommandLine.Option(names = {"-m", "--max-total"}, description = "Pool max size (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
    @CommandLine.Option(names = {"-b", "--block"}, description = "Block until connection is ready (default: ${DEFAULT-VALUE})")
    private boolean blockWhenExhausted = BaseObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED;
    @CommandLine.Option(names = {"-w", "--max-wait"}, description = "Pool max wait (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long maxWaitMillis = BaseObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
    @CommandLine.Option(names = {"-e", "--evict-period"}, description = "Millis between eviction runs (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long timeBetweenEvictionRunsMillis = BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @CommandLine.Option(names = {"-x", "--evict-tests"}, description = "# conns to check per eviction run (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int testsPerEvictionRun = BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new ClusterTLS()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        redisURI = RedisURI.Builder.redis(host).withPort(port).withPassword(password.toCharArray()).build();
        redisURI.setSsl(true);
        redisURI.setVerifyPeer(false);
        RedisClusterClient client = RedisClusterClient.create(redisURI);

        GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>> poolConfig = poolConfig();
        log.info("Creating connection pool: {}", poolConfig);
        GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(() -> client.connect(), poolConfig);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        for (int index = 0; index < threads; index++) {
            executor.submit(new Worker(pool, redisURI.getTimeout().getSeconds(), iterations, pipeline));
        }
        executor.shutdown();
        try {
            executor.awaitTermination(redisURI.getTimeout().getSeconds() * threads, TimeUnit.SECONDS);
            pool.close();
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for workers to complete", e);
        }
    }

    private <T> GenericObjectPoolConfig<T> poolConfig() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();

        // Connection tests
        // To be able to run the while idle test Jedis Pool must set the evictor
        // thread (in "general" section). We will also set the pool to be static
        // so no idle connections could get evicted.

        // Send Redis PING on borrow
        // Recommendation (False), reason - additional RTT on the conn exactly
        // when the app needs it, reduces performance.
        config.setTestOnBorrow(false);

        // Send Redis PING on create
        // Recommendation (False), reason - password makes it completely
        // redundant as Jedis sends AUTH
        config.setTestOnCreate(false);

        // Send Redis PING on return
        // Recommendation (False), reason - the conn will get tested with
        // the Idle test. No real need here. No impact for true as well.
        config.setTestOnReturn(false);

        // Send periodic Redis PING for idle pool connections
        // Recommendation (True), reason - test and heal connections while
        // they are idle in the pool.
        config.setTestWhileIdle(true);

        // Dynamic pool configuration
        // This is advanced configuration and the suggestion for most use-cases
        // is to leave the pool static
        // If you need your pool to be dynamic make sure you understand the
        // configuration options

        config.setMaxIdle(-1);
        config.setMinIdle(-1);
        config.setEvictorShutdownTimeoutMillis(-1);
        config.setMinEvictableIdleTimeMillis(-1);
        config.setSoftMinEvictableIdleTimeMillis(-1);

        // Advanced Evictor and JMX configurations (only touch if you know what you are doing)
        // poolConfig.setEvictionPolicy();
        // poolConfig.setEvictionPolicyClassName();
        // poolConfig.setJmxEnabled();
        // poolConfig.setJmxNameBase();
        // poolConfig.setJmxNameBase();

        // Scheduling algorithms (Leave the defaults)

        // Set to true to have LIFO behavior (always returning the most recently
        // used object from the pool). Set to false to have FIFO behavior
        // Recommendation (?) Default value is True and for now is also the
        // recommendation
        config.setLifo(true);
        // Returns whether or not the pool serves threads waiting to borrow
        // objects fairly.
        // True means that waiting threads are served as if waiting in a FIFO
        // queue.
        // False ??maybe?? relies on the OS scheduling
        // Recommendation (?) Default value is False and for now is also
        // the recommendation
        config.setFairness(false);

        // General configuration
        // This is the application owner part to configure

        // Pool max size
        config.setMaxTotal(maxTotal);
        // True - will block the thread requesting a connection from the pool
        // until a connection is ready (or until timeout - "MaxWaitMillis")
        // False - will immediately return an error
        config.setBlockWhenExhausted(blockWhenExhausted);
        // The maximum amount of time (in milliseconds) the borrowObject()
        // method should block before throwing an exception when the pool is
        // exhausted and getBlockWhenExhausted() is true.
        // When less than 0, the borrowObject() method may block indefinitely.
        config.setMaxWaitMillis(maxWaitMillis);
        // The following EvictionRun parameters must be enabled (positive
        // values) in order to enable the evictor thread.
        // The number of milliseconds to sleep between runs of the idle object
        // evictor thread.
        // When positive, the idle object evictor thread starts.
        // Recommendation (>0) A good start is 1000 (one second)
        config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        // Number of conns to check each eviction run. Positive value is
        // absolute number of conns to check,
        // negative sets a portion to be checked ( -n means about 1/n of the
        // idle connections in the pool will be checked)
        // Recommendation (!=0) A good start is around fifth.
        config.setNumTestsPerEvictionRun(testsPerEvictionRun);

        return config;
    }

    public static class Worker implements Runnable {

        private static final Logger log = LoggerFactory.getLogger(Worker.class);

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final long timeout;
        private final int iterations;
        private final int pipeline;

        public Worker(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, long timeout, int iterations, int pipeline) {
            this.pool = pool;
            this.timeout = timeout;
            this.iterations = iterations;
            this.pipeline = pipeline;
        }

        @Override
        public void run() {
            log.info("Running {} iterations with pipeline={}", iterations, pipeline);
            int count = 0;
            for (int iteration = 0; iteration < iterations; iteration++) {
                log.debug("Getting connection from pool");
                try (StatefulRedisClusterConnection<String, String> connection = pool.borrowObject()) {

                    RedisAdvancedClusterAsyncCommands<String, String> commands = connection.async();
                    commands.setAutoFlushCommands(false); // disable auto-flushing
                    List<RedisFuture<?>> futures = new ArrayList<>(); // perform a series of independent calls
                    for (int index = 0; index < pipeline; index++) {
                        futures.add(commands.set("key-" + Thread.currentThread().getName() + "-" + iteration + "-" + index, "value-" + index));
                    }
                    commands.flushCommands(); // write all commands to the transport layer
                    for (RedisFuture<?> future : futures) {
                        try {
                            future.get(timeout, TimeUnit.SECONDS); // synchronization example: Wait for each future to complete
                            count++;
                        } catch (Exception e) {
                            log.error("Could not get result", e);
                        }
                    }
                } catch (Exception e) {
                    log.error("Could not get connection from pool", e);
                }
            }
            log.info("Finished - Executed {} commands", count);
        }
    }

}
