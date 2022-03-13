package com.github.ymwangzq.config.adapter.core;

import static com.github.phantomthief.util.MoreFunctions.runCatching;
import static com.github.phantomthief.util.MoreFunctions.runThrowing;
import static com.github.phantomthief.util.MoreFunctions.throwing;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.zookeeper.ZkBasedNodeResource;
import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpProxy;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.recipes.ZkBasedNodeResourceConfigAdapter;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author myco
 * Created on 2019-11-11
 */
class ConfigAdapterTestHelperTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfigAdapterTestHelperTest.class);

    private static final String ZK_KEY_PATH2 = "/myco/test/path2";
    private static int ZK_DELAY_S = 1;

    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

    private final ListeningExecutorService zkRefreshExecutor = MoreExecutors.listeningDecorator(newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("zk-refresh-thread")
            .build()));

    @BeforeAll
    static void beforeAll() throws Throwable {
        testingServer = new TestingServer();
        testingServer.start();
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(testingServer.getConnectString())
                .retryPolicy(new RetryOneTime(100))
                .build();
        curatorFramework.start();
    }

    @AfterAll
    static void afterAll() throws Throwable {
        curatorFramework.close();
        testingServer.close();
    }

    private static void setZk(String zkPath, String data) {
        throwing(() -> curatorFramework.create().orSetData().creatingParentsIfNeeded().forPath(zkPath, data.getBytes()));
    }

    private static class StringWrapper {
        private final String s;

        StringWrapper(String s) {
            this.s = s;
        }

        String getS() {
            return s;
        }
    }

    @Test
    void testNotifyUpdate() {
        ConfigAdapter<StringWrapper> zkConfigAdapter2 =
                ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<StringWrapper>newGenericBuilder()
                                .withCacheFactory(ZK_KEY_PATH2, curatorFramework)
                                .withStringFactoryEx(StringWrapper::new)
                                .asyncRefresh(zkRefreshExecutor), newSingleThreadExecutor(),
                        60000);

        AtomicInteger onlyMapOnUpstreamChangeCounter = new AtomicInteger(0);

        ConfigAdapter<String> onlyMapOnUpstreamChange = zkConfigAdapter2.map(s -> {
            onlyMapOnUpstreamChangeCounter.incrementAndGet();
            return Optional.ofNullable(s).map(StringWrapper::getS).orElse(null);
        });

        AtomicInteger mapOnEachUpdateCounter = new AtomicInteger(0);

        ConfigAdapter<String> mapOnEachUpdate =
                ConfigAdapters.toMapOnEachUpdateConfigAdapter(zkConfigAdapter2).map(s -> {
                    mapOnEachUpdateCounter.incrementAndGet();
                    return Optional.ofNullable(s).map(StringWrapper::getS).orElse(null);
                });

        onlyMapOnUpstreamChange.get();
        mapOnEachUpdate.get();

        ConfigAdapterTestHelper.dispatchEvent(BaseEvent.newUpdateEvent());
        System.out.println(onlyMapOnUpstreamChangeCounter.get());
        System.out.println(mapOnEachUpdateCounter.get());
        Assertions.assertEquals(1, onlyMapOnUpstreamChangeCounter.get());
        Assertions.assertTrue(mapOnEachUpdateCounter.get() >= 2);

        ConfigAdapterTestHelper.dispatchEvent(BaseEvent.newUpdateEvent());
        System.out.println(onlyMapOnUpstreamChangeCounter.get());
        System.out.println(mapOnEachUpdateCounter.get());
        Assertions.assertEquals(1, onlyMapOnUpstreamChangeCounter.get());
        Assertions.assertTrue(mapOnEachUpdateCounter.get() >= 3);

        String newZkValue = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH2, newZkValue);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        System.out.println(onlyMapOnUpstreamChangeCounter.get());
        System.out.println(mapOnEachUpdateCounter.get());
        Assertions.assertEquals(2, onlyMapOnUpstreamChangeCounter.get());
        Assertions.assertTrue(mapOnEachUpdateCounter.get() >= 4);

        ConfigAdapterTestHelper.dispatchEvent(BaseEvent.newUpdateEvent());

        System.out.println(onlyMapOnUpstreamChangeCounter.get());
        System.out.println(mapOnEachUpdateCounter.get());
        Assertions.assertEquals(2, onlyMapOnUpstreamChangeCounter.get());
        Assertions.assertTrue(mapOnEachUpdateCounter.get() >= 5);
    }

    @Test
    void testAutoRefresh() {
        ConfigAdapterTestHelper.enableAutoRefresh(Duration.ofMillis(1));
        try {
            AtomicReference<String> data = new AtomicReference<>("");

            ConfigAdapter<String> configAdapter = SupplierConfigAdapter.createConfigAdapter(data::get);

            Assertions.assertEquals("", configAdapter.get());

            String newValue = RandomStringUtils.randomAlphabetic(10);
            data.set(newValue);

            Assertions.assertEquals("", configAdapter.get());
            sleepUninterruptibly(5, MILLISECONDS);
            Assertions.assertEquals(newValue, configAdapter.get());

            ConfigAdapterTestHelper.disableAutoRefresh();
            sleepUninterruptibly(10, MILLISECONDS);

            String newValue2 = RandomStringUtils.randomAlphabetic(10);
            data.set(newValue2);

            Assertions.assertEquals(newValue, configAdapter.get());
            sleepUninterruptibly(2, MILLISECONDS);
            Assertions.assertEquals(newValue, configAdapter.get());
            // 虽然高频自动刷新关掉了，但是默认的每秒刷新还有的
            sleepUninterruptibly(1100, MILLISECONDS);
            Assertions.assertEquals(newValue2, configAdapter.get());
        } finally {
            ConfigAdapterTestHelper.disableAutoRefresh();
        }
    }

    static class Resource {
        private static AtomicInteger counter = new AtomicInteger(0);
        private static AtomicInteger max = new AtomicInteger(0);

        public Resource(CountDownLatch latch) throws Throwable {
            counter.incrementAndGet();
            max.incrementAndGet();
            latch.await();
            sleepUninterruptibly(1, SECONDS);
        }

        public void close() {
            counter.decrementAndGet();
        }

        public static int getCount() {
            return counter.get();
        }

        public static int getMax() {
            return max.get();
        }
    }

    @Test
    void testInitAndCleanUp() {
        int coreNum = Runtime.getRuntime().availableProcessors();
        ExecutorService getterExecutor = Executors.newFixedThreadPool(coreNum);
        Executor executor = Executors.newSingleThreadExecutor();
        // warmup
        executor.execute(() -> {
        });
        BaseEventSource baseEventSource = new BaseEventSource();

        ConfigAdapter<Resource> configAdapter =
                ConfigAdapters.createMapOnEachUpdate(baseEventSource, new Supplier<Resource>() {
                    private volatile boolean firstGet = true;

                    @Override
                    public Resource get() {
                        try {
                            if (firstGet) {
                                CountDownLatch latch = new CountDownLatch(1);
                                firstGet = false;
                                executor.execute(() -> {
                                    latch.countDown();
                                    baseEventSource.dispatchEvent(BaseEvent.newUpdateEvent());
                                });
                                return new Resource(latch);
                            } else {
                                return new Resource(new CountDownLatch(0));
                            }
                        } catch (Throwable t) {
                            Throwables.throwIfUnchecked(t);
                            throw new RuntimeException(t);
                        }
                    }
                }, r -> Optional.ofNullable(r).ifPresent(Resource::close));

        CountDownLatch latch = new CountDownLatch(1);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < coreNum * 10; i++) {
            futures.add(getterExecutor.submit(() -> {
                runThrowing(latch::await);
                configAdapter.get();
            }));
        }
        latch.countDown();
        futures.forEach(f -> runCatching(f::get));
        sleepUninterruptibly(2, SECONDS);

        Assertions.assertEquals(1, Resource.getCount());
        Assertions.assertEquals(2, Resource.getMax());
    }

    @Test
    void testDetachOnlyInterruptSelfFetcher() {
        Thread.interrupted();
        AtomicReference<String> dataSource = new AtomicReference<>();

        BaseEventSource baseEventSource = new BaseEventSource();
        ConfigAdapter<String> configAdapter =
                ConfigAdapters.createMapOnEachUpdate(baseEventSource, dataSource::get, s -> {
                });

        RateLimiter rateLimiter = RateLimiter.create(1);

        AtomicBoolean stop = new AtomicBoolean(false);
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 5; i++) {
            ConfigAdapter<String> tempConfigAdapter = configAdapter;
            executorService.submit(() -> {
                while (!stop.get()) {
                    tempConfigAdapter.map(s -> Optional.ofNullable(s).map(String::hashCode).orElse(null)).get();
                    if (rateLimiter.tryAcquire()) {
                        System.gc();
                    }
                    sleepUninterruptibly(10, NANOSECONDS);
                }
            });
        }

        ConfigAdapter<String> configAdapter1 = configAdapter.map(s -> "config1: " + s);
        CyclicBarrier barrier = new CyclicBarrier(2);
        configAdapter1.addEventListener(EventListeners.updateEventListener(() -> {
            try {
                barrier.await(1, SECONDS);
            } catch (Throwable t) {
                logger.error("", t);
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }));
        configAdapter1.get();

        RateLimiter rateLimiter2 = RateLimiter.create(1);

        int loop = 30000;
        for (int i = 0; i < loop; i++) {
            String value = RandomStringUtils.randomAlphanumeric(10);
            dataSource.set(value);
            executorService.submit(() -> baseEventSource.dispatchEvent(BaseEvent.newUpdateEvent()));
            try {
                barrier.await(1, SECONDS);
            } catch (Throwable t) {
                logger.error("", t);
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
            if (rateLimiter2.tryAcquire()) {
                logger.info("i = [{}]", i);
            }
            Assertions.assertEquals("config1: " + value, configAdapter1.get());
        }

        configAdapter1 = null;
        stop.set(true);

        System.out.println(configAdapter.get());
        configAdapter = null;

        for (int i = 0; i < 20 && GcUtilHelper.getFinalizerMapSize() > 2; i++) {
            GcUtilHelper.doBatchFinalize();
            System.gc();
            System.out.println("done: " + GcUtilHelper.getFinalizerMapSize());
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        Assertions.assertTrue(GcUtilHelper.getFinalizerMapSize() <= 2);
    }

    @Test
    void testManuallyUpdate() {
        AtomicReference<String> data = new AtomicReference<>("version1");

        ConfigAdapter<String> configAdapter = SupplierConfigAdapter.createConfigAdapter(data::get)
                .map(s -> {
                    sleepUninterruptibly(1, SECONDS);
                    return s;
                });

        Assertions.assertEquals("version1", configAdapter.get());
        data.set("version2");
        ConfigAdapterTestHelper.dispatchEvent(BaseEvent.newUpdateEvent());
        Assertions.assertEquals("version2", configAdapter.get());
    }

    static class Pojo2 {
        private static final AtomicInteger CLEAN_UP_COUNTER = new AtomicInteger(0);

        void cleanUp() {
            CLEAN_UP_COUNTER.incrementAndGet();
        }

        static int getCleanUpCounter() {
            return CLEAN_UP_COUNTER.get();
        }

        static void clearCleanUpCounter() {
            CLEAN_UP_COUNTER.set(0);
        }
    }

    static class Pojo2Proxy extends Pojo2 implements AdaptiveCleanUpProxy<Pojo2> {
        private final Pojo2 delegate;

        public Pojo2Proxy(Pojo2 delegate) {
            this.delegate = delegate;
        }

        @Override
        public Pojo2 getDelegate() {
            return delegate;
        }
    }

    @Test
    void testAdaptiveCleanUp() {
        Pojo2.clearCleanUpCounter();

        AtomicReference<Pojo2Proxy> data = new AtomicReference<>();
        data.set(new Pojo2Proxy(new Pojo2()));

        BaseEventSource eventSource = new BaseEventSource();
        Consumer<Pojo2> pojo2Consumer = p -> {
            if (p != null) {
                p.cleanUp();
            }
        };
        ConfigAdapter<Pojo2Proxy> configAdapter = ConfigAdapters.createAdaptiveCleanUp(eventSource, data::get,
                pojo2Consumer);
        Pojo2 pojo2 = configAdapter.get();

        Assertions.assertEquals(0, Pojo2.getCleanUpCounter());
        data.set(null);
        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        System.gc();
        sleepUninterruptibly(2, SECONDS);
        Assertions.assertEquals(0, Pojo2.getCleanUpCounter());
        pojo2 = null;
        System.gc();
        sleepUninterruptibly(2, SECONDS);
        Assertions.assertEquals(1, Pojo2.getCleanUpCounter());
    }

    @Test
    void testAdaptiveCleanUpMap() {
        Pojo2.clearCleanUpCounter();

        AtomicReference<String> data = new AtomicReference<>();
        data.set("");

        BaseEventSource eventSource = new BaseEventSource();
        ConfigAdapter<Pojo2Proxy> configAdapter = ConfigAdapters.create(eventSource, data::get,
                s -> {
                }).map(new AdaptiveCleanUpConfigAdapterMapper<String, Pojo2Proxy, Pojo2>() {
            @Nullable
            @Override
            public Pojo2Proxy map(@Nullable String s) {
                return new Pojo2Proxy(new Pojo2());
            }

            @Override
            public void cleanUp(@Nullable Pojo2 pojo2) {
                if (pojo2 != null) {
                    pojo2.cleanUp();
                }
            }

            @Nullable
            @Override
            public Pojo2 transformToRealCleanUp(@Nullable Pojo2Proxy pojo2Proxy) {
                return Optional.ofNullable(pojo2Proxy).map(Pojo2Proxy::getDelegate).orElse(null);
            }
        });
        Pojo2 pojo2 = configAdapter.get();

        Assertions.assertEquals(0, Pojo2.getCleanUpCounter());
        data.set(null);
        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        System.gc();
        sleepUninterruptibly(2, SECONDS);
        Assertions.assertEquals(0, Pojo2.getCleanUpCounter());
        pojo2 = null;
        System.gc();
        sleepUninterruptibly(2, SECONDS);
        Assertions.assertEquals(1, Pojo2.getCleanUpCounter());
    }

    static class Pojo2ProxyError extends Pojo2 implements AdaptiveCleanUpProxy<String> {

        public Pojo2ProxyError() {
        }

        @Override
        public String getDelegate() {
            return "";
        }
    }

    @Test
    void testAdaptiveCleanUpErrorUsage() {

        Pojo2.clearCleanUpCounter();

        AtomicReference<Pojo2ProxyError> data = new AtomicReference<>();
        data.set(new Pojo2ProxyError());

        BaseEventSource eventSource = new BaseEventSource();
        ConfigAdapter<Pojo2ProxyError> configAdapter = ConfigAdapters.create(eventSource, data::get, p -> {
            if (p != null) {
                p.cleanUp();
            }
        });
        Pojo2ProxyError pojo2 = configAdapter.get();

        Assertions.assertEquals(0, Pojo2.getCleanUpCounter());
        data.set(null);
        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());

        Assertions.assertEquals(1, Pojo2.getCleanUpCounter());
    }

    @Test
    void testEventuallyConsistency() {
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicLong data = new AtomicLong(1);
        ConfigAdapter<Long> configAdapter = SupplierConfigAdapter.createConfigAdapter(data::get);
        ConfigAdapter<Long> configAdapter1 = configAdapter.map(UpdateWithRetryMapper.wrap(l -> {
            if (l == null) {
                throw new RuntimeException("");
            }
            if (l % 3 == 0) {
                throw new RuntimeException("mod3 == 0");
            }
            return l;
        }, (retryTime, lastTryException) -> 1000));

        Assertions.assertEquals(1, configAdapter1.get());

        AtomicReference<CountDownLatch> latchAtomicReference = new AtomicReference<>();

        executorService.submit(() -> {
            while (true) {
                data.incrementAndGet();
                //noinspection UnstableApiUsage
                sleepUninterruptibly(Duration.ofNanos(ThreadLocalRandom.current().nextInt(1, 100)));
                CountDownLatch latch = latchAtomicReference.get();
                latchAtomicReference.compareAndSet(latch, null);
                if (latch != null) {
                    runThrowing(latch::await);
                }
            }
        });

        for (int i = 0; i < 100; i++) {
            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), MILLISECONDS);
            CountDownLatch latch = new CountDownLatch(1);
            latchAtomicReference.set(latch);
            Uninterruptibles.sleepUninterruptibly(1, MICROSECONDS);
            if (data.get() % 3 != 0) {
                while (configAdapter1.get() != data.get()) {
                    System.out.println("i: " + i + " config: " + data.get() + " value: " + configAdapter1.get());
                    Uninterruptibles.sleepUninterruptibly(1, SECONDS);
                }
                System.out.println("i: " + i + " config: " + data.get() + " value: " + configAdapter1.get());
            } else {
                Uninterruptibles.sleepUninterruptibly(1, SECONDS);
            }
            latch.countDown();
        }
    }
}