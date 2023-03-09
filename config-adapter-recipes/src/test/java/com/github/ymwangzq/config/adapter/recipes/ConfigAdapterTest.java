package com.github.ymwangzq.config.adapter.recipes;

import static com.github.phantomthief.util.MoreFunctions.throwing;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.zookeeper.GenericZkBasedNodeBuilder;
import com.github.phantomthief.zookeeper.ZkBasedNodeResource;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.github.ymwangzq.config.adapter.core.BaseEvent;
import com.github.ymwangzq.config.adapter.core.BaseEventSource;
import com.github.ymwangzq.config.adapter.core.ConfigAdapters;
import com.github.ymwangzq.config.adapter.core.EventListeners;
import com.github.ymwangzq.config.adapter.core.MapWithLastValueMapper;
import com.github.ymwangzq.config.adapter.core.SupplierConfigAdapter;
import com.github.ymwangzq.config.adapter.core.UpdateWithRetryMapper;
import com.github.ymwangzq.config.adapter.core.UpdateWithRetryMapper.RetryConfig;
import com.github.ymwangzq.config.adapter.core.UpdateWithRetryMapper.RetryForever;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.DelayCleanUpConfig;

/**
 * @author myco
 * Created on 2019-10-23
 */
@TestMethodOrder(OrderAnnotation.class)
class ConfigAdapterTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfigAdapterTest.class);

    private static final String ZK_KEY_PATH = "/myco/test/path";
    private static int ZK_DELAY_S = 1;

    private static TestingServer testZkServer;

    private static CuratorFramework curatorFramework;

    static void setZk(String zkPath, String data) {
        throwing(() -> curatorFramework.create().orSetData().creatingParentsIfNeeded()
                .forPath(zkPath, data.getBytes()));
    }

    private static void delZk(String zkPath) {
        try {
            if (curatorFramework.checkExists().forPath(zkPath) != null) {
                throwing(() -> curatorFramework.delete().deletingChildrenIfNeeded().forPath(zkPath));
            }
        } catch (Throwable t) {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    static final AtomicReference<String> configSource = new AtomicReference<>(null);
    static final ConfigAdapter<String> SIMPLE_CONFIG_ADAPTER = SupplierConfigAdapter.createConfigAdapter(configSource::get);

    private final ListeningExecutorService zkRefreshExecutor =
            MoreExecutors.listeningDecorator(newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("zk-refresh-thread")
                    .build()));
    private final ConfigAdapter<String> zkConfigAdapter =
            ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                            .withCacheFactory(ZK_KEY_PATH, curatorFramework)
                            .withStringFactoryEx(s -> s)
                            .asyncRefresh(zkRefreshExecutor)
                    , newSingleThreadExecutor(),
                    5000);

    @BeforeAll
    static void setUp() throws Throwable {
        testZkServer = new TestingServer();
        testZkServer.start();
        Uninterruptibles.sleepUninterruptibly(1, SECONDS);
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(testZkServer.getConnectString())
                .retryPolicy(new RetryOneTime(100))
                .build();
        curatorFramework.start();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleWithFixedDelay(System::gc, 1, 1, SECONDS);
        Uninterruptibles.sleepUninterruptibly(1, SECONDS);
    }

    @Order(1)
    @Test
    void testSimple() {
        configSource.set("{}");

        ConfigAdapter<Integer> configAdapter1 =
                SIMPLE_CONFIG_ADAPTER.map(f -> Optional.ofNullable(f).map(String::length).orElse(null));
        ConfigAdapter<Integer> configAdapter11 =
                configAdapter1.map(f -> Optional.ofNullable(f).map(i -> i * 2).orElse(null));
        ConfigAdapter<String> configAdapter2 =
                SIMPLE_CONFIG_ADAPTER.map(f -> Optional.ofNullable(f).map(s -> s + " | " + s).orElse(null));

        System.out.println(SIMPLE_CONFIG_ADAPTER.get());
        System.out.println(configAdapter1.get());
        System.out.println(configAdapter11.get());
        System.out.println(configAdapter2.get());

        Assertions.assertEquals("{}", SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals("{}".length(), configAdapter1.get());
        Assertions.assertEquals("{}".length() * 2, configAdapter11.get());
        Assertions.assertEquals("{} | {}", configAdapter2.get());

        String newValue = RandomStringUtils.randomAlphabetic(10);
        configSource.set(newValue);
        Uninterruptibles.sleepUninterruptibly(2, SECONDS);

        System.out.println(SIMPLE_CONFIG_ADAPTER.get());
        System.out.println(configAdapter1.get());
        System.out.println(configAdapter11.get());
        System.out.println(configAdapter2.get());

        Assertions.assertEquals(newValue, SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals(newValue.length(), configAdapter1.get());
        Assertions.assertEquals(newValue.length() * 2, configAdapter11.get());
        Assertions.assertEquals(newValue + " | " + newValue, configAdapter2.get());
    }

    @Order(2)
    @Test
    void testZk() {

        setZk(ZK_KEY_PATH, "{}");
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        ConfigAdapter<Integer> configAdapter1 =
                zkConfigAdapter.map(f -> Optional.ofNullable(f).map(String::length).orElse(null));
        ConfigAdapter<Integer> configAdapter11 =
                configAdapter1.map(f -> Optional.ofNullable(f).map(i -> i * 2).orElse(null));
        ConfigAdapter<String> configAdapter2 =
                zkConfigAdapter.map(f -> Optional.ofNullable(f).map(s -> s + " | " + s).orElse(null));

        System.out.println(zkConfigAdapter.get());
        System.out.println(configAdapter1.get());
        System.out.println(configAdapter11.get());
        System.out.println(configAdapter2.get());

        Assertions.assertEquals("{}", zkConfigAdapter.get());
        Assertions.assertEquals("{}".length(), configAdapter1.get());
        Assertions.assertEquals("{}".length() * 2, configAdapter11.get());
        Assertions.assertEquals("{} | {}", configAdapter2.get());

        String newValue = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newValue);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        System.out.println(zkConfigAdapter.get());
        System.out.println(configAdapter1.get());
        System.out.println(configAdapter11.get());
        System.out.println(configAdapter2.get());

        Assertions.assertEquals(newValue, zkConfigAdapter.get());
        Assertions.assertEquals(newValue.length(), configAdapter1.get());
        Assertions.assertEquals(newValue.length() * 2, configAdapter11.get());
        Assertions.assertEquals(newValue + " | " + newValue, configAdapter2.get());
    }

    @Order(2)
    @Test
    void testZkNodeNotExist() {
        String ZK_KEY_NOT_EXIST_PATH = "/config/test/mycoNodeNotExistTest";
        delZk(ZK_KEY_NOT_EXIST_PATH);
        ConfigAdapter<String> zkNoNodeConfigAdapter =
                ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                                .withCacheFactory(ZK_KEY_NOT_EXIST_PATH, curatorFramework)
                                .withStringFactoryEx(s -> s)
                                .asyncRefresh(zkRefreshExecutor)
                        , newSingleThreadExecutor(),
                        5000);

        Assertions.assertNull(zkNoNodeConfigAdapter.get());

        String value1 = "value1";
        setZk(ZK_KEY_NOT_EXIST_PATH, value1);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(value1, zkNoNodeConfigAdapter.get());

        String value2 = "value2";
        setZk(ZK_KEY_NOT_EXIST_PATH, value2);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(value2, zkNoNodeConfigAdapter.get());

        delZk(ZK_KEY_NOT_EXIST_PATH);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertNull(zkNoNodeConfigAdapter.get());
    }

    @Order(3)
    @Test
    void testMergeConfigs() {

        setZk(ZK_KEY_PATH, "{}");
        configSource.set("{}");
        sleepUninterruptibly(2, TimeUnit.SECONDS);

        ConfigAdapter<String> mergedConfigAdapter = ConfigAdapters
                .mergeTwoConfigAdapters(zkConfigAdapter, SIMPLE_CONFIG_ADAPTER, (f1, f2) -> f1 + " | " + f2, s -> {
                });

        ConfigAdapter<Integer> mergedConfigAdapter1 =
                mergedConfigAdapter.map(f -> Optional.ofNullable(f).map(String::length).orElse(null));
        ConfigAdapter<Integer> mergedConfigAdapter11 =
                mergedConfigAdapter1.map(f -> Optional.ofNullable(f).map(i -> i * 2).orElse(null));
        ConfigAdapter<String> mergedConfigAdapter2 =
                mergedConfigAdapter.map(f -> Optional.ofNullable(f).map(s -> s + " | " + s).orElse(null));

        ConfigAdapter<Integer> zkConfigAdapter1 =
                zkConfigAdapter.map(f -> Optional.ofNullable(f).map(String::length).orElse(null));
        ConfigAdapter<Integer> zkConfigAdapter11 =
                zkConfigAdapter1.map(f -> Optional.ofNullable(f).map(i -> i * 2).orElse(null));
        ConfigAdapter<String> zkConfigAdapter2 =
                zkConfigAdapter.map(f -> Optional.ofNullable(f).map(s -> s + " | " + s).orElse(null));

        ConfigAdapter<Integer> configAdapter1 =
                SIMPLE_CONFIG_ADAPTER.map(f -> Optional.ofNullable(f).map(String::length).orElse(null));
        ConfigAdapter<Integer> configAdapter11 =
                configAdapter1.map(f -> Optional.ofNullable(f).map(i -> i * 2).orElse(null));
        ConfigAdapter<String> configAdapter2 =
                SIMPLE_CONFIG_ADAPTER.map(f -> Optional.ofNullable(f).map(s -> s + " | " + s).orElse(null));

        System.out.println("========= zk  =========");
        System.out.println(zkConfigAdapter.get());
        System.out.println(zkConfigAdapter1.get());
        System.out.println(zkConfigAdapter11.get());
        System.out.println(zkConfigAdapter2.get());
        System.out.println("========= simpleConfigAdapter  =========");
        System.out.println(SIMPLE_CONFIG_ADAPTER.get());
        System.out.println(configAdapter1.get());
        System.out.println(configAdapter11.get());
        System.out.println(configAdapter2.get());
        System.out.println("========= merged  =========");
        System.out.println(mergedConfigAdapter.get());
        System.out.println(mergedConfigAdapter1.get());
        System.out.println(mergedConfigAdapter11.get());
        System.out.println(mergedConfigAdapter2.get());

        Assertions.assertEquals("{}", zkConfigAdapter.get());
        Assertions.assertEquals("{}".length(), zkConfigAdapter1.get());
        Assertions.assertEquals("{}".length() * 2, zkConfigAdapter11.get());
        Assertions.assertEquals("{} | {}", zkConfigAdapter2.get());

        Assertions.assertEquals("{}", SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals("{}".length(), configAdapter1.get());
        Assertions.assertEquals("{}".length() * 2, configAdapter11.get());
        Assertions.assertEquals("{} | {}", configAdapter2.get());

        String mergedValue = "{} | {}";
        Assertions.assertEquals(mergedValue, mergedConfigAdapter.get());
        Assertions.assertEquals(mergedValue.length(), mergedConfigAdapter1.get());
        Assertions.assertEquals(mergedValue.length() * 2, mergedConfigAdapter11.get());
        Assertions.assertEquals(mergedValue + " | " + mergedValue, mergedConfigAdapter2.get());

        String newZkValue = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newZkValue);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        System.out.println("========= zk  =========");
        System.out.println(zkConfigAdapter.get());
        System.out.println(zkConfigAdapter1.get());
        System.out.println(zkConfigAdapter11.get());
        System.out.println(zkConfigAdapter2.get());
        System.out.println("========= simpleConfigAdapter  =========");
        System.out.println(SIMPLE_CONFIG_ADAPTER.get());
        System.out.println(configAdapter1.get());
        System.out.println(configAdapter11.get());
        System.out.println(configAdapter2.get());
        System.out.println("========= merged  =========");
        System.out.println(mergedConfigAdapter.get());
        System.out.println(mergedConfigAdapter1.get());
        System.out.println(mergedConfigAdapter11.get());
        System.out.println(mergedConfigAdapter2.get());

        Assertions.assertEquals(newZkValue, zkConfigAdapter.get());
        Assertions.assertEquals(newZkValue.length(), zkConfigAdapter1.get());
        Assertions.assertEquals(newZkValue.length() * 2, zkConfigAdapter11.get());
        Assertions.assertEquals(newZkValue + " | " + newZkValue, zkConfigAdapter2.get());

        Assertions.assertEquals("{}", SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals("{}".length(), configAdapter1.get());
        Assertions.assertEquals("{}".length() * 2, configAdapter11.get());
        Assertions.assertEquals("{} | {}", configAdapter2.get());

        mergedValue = newZkValue + " | {}";
        Assertions.assertEquals(mergedValue, mergedConfigAdapter.get());
        Assertions.assertEquals(mergedValue.length(), mergedConfigAdapter1.get());
        Assertions.assertEquals(mergedValue.length() * 2, mergedConfigAdapter11.get());
        Assertions.assertEquals(mergedValue + " | " + mergedValue, mergedConfigAdapter2.get());

        String newValue = RandomStringUtils.randomAlphabetic(10);
        configSource.set(newValue);
        sleepUninterruptibly(2, TimeUnit.SECONDS);

        System.out.println("========= zk  =========");
        System.out.println(zkConfigAdapter.get());
        System.out.println(zkConfigAdapter1.get());
        System.out.println(zkConfigAdapter11.get());
        System.out.println(zkConfigAdapter2.get());
        System.out.println("========= simpleConfigAdapter  =========");
        System.out.println(SIMPLE_CONFIG_ADAPTER.get());
        System.out.println(configAdapter1.get());
        System.out.println(configAdapter11.get());
        System.out.println(configAdapter2.get());
        System.out.println("========= merged  =========");
        System.out.println(mergedConfigAdapter.get());
        System.out.println(mergedConfigAdapter1.get());
        System.out.println(mergedConfigAdapter11.get());
        System.out.println(mergedConfigAdapter2.get());

        Assertions.assertEquals(newZkValue, zkConfigAdapter.get());
        Assertions.assertEquals(newZkValue.length(), zkConfigAdapter1.get());
        Assertions.assertEquals(newZkValue.length() * 2, zkConfigAdapter11.get());
        Assertions.assertEquals(newZkValue + " | " + newZkValue, zkConfigAdapter2.get());

        Assertions.assertEquals(newValue, SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals(newValue.length(), configAdapter1.get());
        Assertions.assertEquals(newValue.length() * 2, configAdapter11.get());
        Assertions.assertEquals(newValue + " | " + newValue, configAdapter2.get());

        mergedValue = newZkValue + " | " + newValue;
        Assertions.assertEquals(mergedValue, mergedConfigAdapter.get());
        Assertions.assertEquals(mergedValue.length(), mergedConfigAdapter1.get());
        Assertions.assertEquals(mergedValue.length() * 2, mergedConfigAdapter11.get());
        Assertions.assertEquals(mergedValue + " | " + mergedValue, mergedConfigAdapter2.get());
    }

    private static class CallTimeCounter {

        private static final AtomicInteger cleanUpCallNum = new AtomicInteger();
        private static final AtomicInteger constructorCallNum = new AtomicInteger();

        private volatile boolean closed = false;
        private final String data;

        CallTimeCounter() {
            this(null);
        }

        CallTimeCounter(String data) {
            this.data = data;
            constructorCallNum.incrementAndGet();
        }

        public String getData() {
            return data;
        }

        void close() {
            closed = true;
            cleanUpCallNum.incrementAndGet();
        }

        boolean isClosed() {
            return closed;
        }

        static int getConstructorCallNum() {
            return constructorCallNum.get();
        }

        static int getCleanUpCallNum() {
            return cleanUpCallNum.get();
        }

        static void reset() {
            cleanUpCallNum.set(0);
            constructorCallNum.set(0);
        }
    }

    @Order(4)
    @Test
    void testCallTimes() {

        ConfigAdapter<CallTimeCounter> mappedConfig = zkConfigAdapter.map(
                new ConfigAdapterMapper<String, CallTimeCounter>() {
                    @Override
                    public CallTimeCounter map(@Nullable String s) {
                        return new CallTimeCounter();
                    }

                    @Override
                    public void cleanUp(@Nullable CallTimeCounter cleanUpCounter) {
                        if (cleanUpCounter != null) {
                            cleanUpCounter.close();
                        }
                    }
                });
        ConfigAdapter<String> mappedConfig1 = mappedConfig.map(s -> "");

        Assertions.assertEquals(0, CallTimeCounter.getConstructorCallNum());
        Assertions.assertEquals(0, CallTimeCounter.getCleanUpCallNum());

        String newZkValue = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newZkValue);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(0, CallTimeCounter.getConstructorCallNum());
        Assertions.assertEquals(0, CallTimeCounter.getCleanUpCallNum());

        Assertions.assertFalse(Objects.requireNonNull(mappedConfig.get()).isClosed());
        mappedConfig1.get();

        Assertions.assertEquals(1, CallTimeCounter.getConstructorCallNum());
        Assertions.assertEquals(0, CallTimeCounter.getCleanUpCallNum());

        String newZkValue2 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newZkValue2);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(2, CallTimeCounter.getConstructorCallNum());
        Assertions.assertEquals(1, CallTimeCounter.getCleanUpCallNum());

        Assertions.assertFalse(Objects.requireNonNull(mappedConfig.get()).isClosed());

    }

    @Order(5)
    @Test
    void testDelayCleanUpWhenUpdateFail() {
        Map<String, CloseableResourceWithData> resourceWithDataMap = Maps.newConcurrentMap();

        ConfigAdapter<CloseableResourceWithData> resource =
                zkConfigAdapter.map(new ConfigAdapterMapper<String, CloseableResourceWithData>() {
                    @Override
                    public CloseableResourceWithData map(@Nullable String s) {
                        CloseableResourceWithData resource = new CloseableResourceWithData(s);
                        resourceWithDataMap.put(s, resource);
                        return resource;
                    }

                    @Override
                    public void cleanUp(@Nullable CloseableResourceWithData closeableResourceWithData) {
                        if (closeableResourceWithData != null) {
                            closeableResourceWithData.close();
                        }
                    }
                });

        AtomicBoolean mapShouldFail = new AtomicBoolean(false);

        ConfigAdapter<String> resource2 = resource.map(new ConfigAdapterMapper<CloseableResourceWithData, String>() {
            @Nullable
            @Override
            public String map(@Nullable CloseableResourceWithData closeableResourceWithData) {
                if (mapShouldFail.get()) {
                    throw new RuntimeException("mapShouldFail is true");
                }
                if (closeableResourceWithData != null) {
                    return closeableResourceWithData.getData();
                } else {
                    return null;
                }
            }
        });

        String value1 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, value1);
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        Assertions.assertEquals(value1, resource2.get());
        Assertions.assertEquals(1, resourceWithDataMap.size());
        Assertions.assertNotNull(resourceWithDataMap.get(value1));
        Assertions.assertFalse(resourceWithDataMap.get(value1).isClosed());

        String value2 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, value2);
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        Assertions.assertEquals(value2, resource2.get());
        Assertions.assertEquals(2, resourceWithDataMap.size());
        Assertions.assertNotNull(resourceWithDataMap.get(value2));
        Assertions.assertFalse(resourceWithDataMap.get(value2).isClosed());
        Assertions.assertTrue(resourceWithDataMap.get(value1).isClosed());

        mapShouldFail.set(true);
        String value3 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, value3);
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        Assertions.assertEquals(value2, resource2.get());
        Assertions.assertEquals(value3, Objects.requireNonNull(resource.get()).getData());
        Assertions.assertEquals(3, resourceWithDataMap.size());
        Assertions.assertNotNull(resourceWithDataMap.get(value3));
        Assertions.assertFalse(resourceWithDataMap.get(value3).isClosed());
        Assertions.assertFalse(resourceWithDataMap.get(value2).isClosed());

        mapShouldFail.set(false);
        setZk(ZK_KEY_PATH, value3);
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        Assertions.assertFalse(resourceWithDataMap.get(value3).isClosed());
        Assertions.assertTrue(resourceWithDataMap.get(value2).isClosed());
    }

    @Order(6)
    @Test
    void testFailListener() {

        AtomicInteger mapperCallTime = new AtomicInteger(0);
        AtomicInteger cleanUpCallTime = new AtomicInteger(0);

        AtomicBoolean mapperShouldFail = new AtomicBoolean(false);
        AtomicBoolean cleanUpShouldFail = new AtomicBoolean(false);

        ConfigAdapter<String> testFailConfigAdapter = zkConfigAdapter.map(new ConfigAdapterMapper<String, String>() {

            @Override
            public String map(@Nullable String s) {
                mapperCallTime.incrementAndGet();
                if (mapperShouldFail.get()) {
                    throw new RuntimeException("fetch fail test Exception");
                }
                return s;
            }

            @Override
            public void cleanUp(@Nullable String s) {
                cleanUpCallTime.incrementAndGet();
                if (cleanUpShouldFail.get()) {
                    throw new RuntimeException("fetch fail test Exception");
                }
            }
        });

        AtomicInteger fetchFailCounter = new AtomicInteger(0);
        AtomicInteger cleanUpFailCounter = new AtomicInteger(0);

        testFailConfigAdapter
                .addEventListener(EventListeners.fetchFailEventListener(fetchFailCounter::incrementAndGet));
        testFailConfigAdapter
                .addEventListener(EventListeners.cleanUpFailEventListener(cleanUpFailCounter::incrementAndGet));

        Assertions.assertEquals(0, mapperCallTime.get());
        Assertions.assertEquals(0, fetchFailCounter.get());
        Assertions.assertEquals(0, cleanUpFailCounter.get());

        testFailConfigAdapter.get();

        Assertions.assertEquals(1, mapperCallTime.get());
        Assertions.assertEquals(0, fetchFailCounter.get());
        Assertions.assertEquals(0, cleanUpFailCounter.get());

        mapperShouldFail.set(true);
        String newValue = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newValue);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(2, mapperCallTime.get());
        Assertions.assertEquals(1, fetchFailCounter.get());
        Assertions.assertEquals(0, cleanUpFailCounter.get());

        mapperShouldFail.set(false);
        cleanUpShouldFail.set(true);
        String newValue2 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newValue2);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(3, mapperCallTime.get());
        Assertions.assertEquals(1, fetchFailCounter.get());
        Assertions.assertEquals(1, cleanUpFailCounter.get());
    }

    @Order(7)
    @Test
    void testNullValue() throws Throwable {
        AtomicInteger mapperCallCounter = new AtomicInteger(0);

        curatorFramework.create().orSetData().creatingParentsIfNeeded().forPath(ZK_KEY_PATH, "aaa".getBytes(StandardCharsets.UTF_8));

        GenericZkBasedNodeBuilder<String> builder = ZkBasedNodeResource.<String>newGenericBuilder()
                .withCacheFactory(ZK_KEY_PATH, curatorFramework)
                .withStringFactoryEx(s -> {
                    mapperCallCounter.incrementAndGet();
                    return null;
                })
                .asyncRefresh(zkRefreshExecutor);

        ZkBasedNodeResource<String> zkBasedNodeResource = builder.build();

        ConfigAdapter<String> zkConfigAdapter =
                ZkBasedNodeResourceConfigAdapter.create(builder, newSingleThreadExecutor(), 5000);

        for (int i = 1; i <= 100; i++) {
            Assertions.assertNull(zkBasedNodeResource.get());
            Assertions.assertEquals(i, mapperCallCounter.get());
        }

        mapperCallCounter.set(0);

        for (int i = 1; i <= 100; i++) {
            Assertions.assertNull(zkConfigAdapter.get());
            Assertions.assertEquals(1, mapperCallCounter.get());
        }

        mapperCallCounter.set(0);

        AtomicReference<String> someConfigSource = new AtomicReference<>(null);
        Supplier<String> someSupplier = new Supplier<String>() {
            private volatile boolean inited = false;
            private volatile String lastValue = null;

            @Override
            public String get() {
                synchronized (this) {
                    if (!inited) {
                        mapperCallCounter.incrementAndGet();
                        inited = true;
                    } else if (!Objects.equals(someConfigSource.get(), lastValue)) {
                        mapperCallCounter.incrementAndGet();
                    }
                    return lastValue;
                }
            }
        };

        ConfigAdapter<String> configAdapter = SupplierConfigAdapter.createConfigAdapter(someSupplier);

        for (int i = 1; i <= 100; i++) {
            Assertions.assertNull(configAdapter.get());
            int count = mapperCallCounter.get();
            Assertions.assertEquals(1, count, "count: " + count);
        }

        for (int i = 1; i <= 100; i++) {
            Assertions.assertNull(someSupplier.get());
            int count = mapperCallCounter.get();
            Assertions.assertEquals(1, count, "count: " + count);
        }
    }

    @Order(8)
    @Test
    void testMultiThreadInit() {
        ExecutorService executorService = Executors
                .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4, new ThreadFactoryBuilder().setDaemon(true).build());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger initCounter = new AtomicInteger(0);

        ConfigAdapter<Pojo1> zkConfigAdapter =
                ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                                        .withCacheFactory(ZK_KEY_PATH, curatorFramework)
                                        .withStringFactoryEx(s -> s)
                                        .asyncRefresh(zkRefreshExecutor), newSingleThreadExecutor(),
                                5000)
                        .map(s -> Optional.ofNullable(s).map(str -> {
                            initCounter.incrementAndGet();
                            sleepUninterruptibly(1, MILLISECONDS);
                            return new Pojo1(s);
                        }).orElse(null));

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    latch.await();
                    zkConfigAdapter.get();
                } catch (Throwable t) {
                    // ignore
                }
            }));
        }
        latch.countDown();
        futures.forEach(future -> {
            try {
                future.get();
            } catch (Throwable t) {
                // ignore
            }
        });
        Assertions.assertEquals(1, initCounter.get());
        executorService.shutdown();
    }

    @Order(9)
    @Test
    void testNotCleanUpSameObject() {
        CallTimeCounter obj = new CallTimeCounter();
        ConfigAdapter<CallTimeCounter> configAdapter = zkConfigAdapter.map(
                new ConfigAdapterMapper<String, CallTimeCounter>() {
                    @Override
                    public CallTimeCounter map(@Nullable String s) {
                        return obj;
                    }

                    @Override
                    public void cleanUp(@Nullable CallTimeCounter callTimeCounter) {
                        if (callTimeCounter != null) {
                            callTimeCounter.close();
                        }
                    }
                });
        String newValue = RandomStringUtils.randomAlphabetic(10);
        Assertions.assertEquals(obj, configAdapter.get());
        setZk(ZK_KEY_PATH, newValue);
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);
        Assertions.assertFalse(Objects.requireNonNull(configAdapter.get()).isClosed());
    }

    private static class CloseableResource {
        private AtomicBoolean closed = new AtomicBoolean(false);

        public boolean isClosed() {
            return closed.get();
        }

        public void close() {
            closed.set(true);
        }
    }

    private static class RefreshAbleService {
        private final CloseableResource resource;

        public RefreshAbleService(CloseableResource resource) {
            this.resource = resource;
        }

        public void doSomeThing() {
            Preconditions.checkState(!resource.isClosed());
        }
    }

    @Order(10)
    @Test
    void testPropagateDelayCleanUp() {
        ConfigAdapter<CloseableResource> closeableResourceConfigAdapter = zkConfigAdapter.map(
                new ConfigAdapterMapper<String, CloseableResource>() {
                    @Override
                    public CloseableResource map(@Nullable String s) {
                        return new CloseableResource();
                    }

                    @Override
                    public void cleanUp(@Nullable CloseableResource closeableResource) {
                        if (closeableResource != null) {
                            closeableResource.close();
                        }
                    }

                    private final ScheduledExecutorService scheduledExecutorService =
                            Executors.newSingleThreadScheduledExecutor();
                    private final DelayCleanUpConfig delayCleanUpConfig = new DelayCleanUpConfig() {
                        @Nonnull
                        @Override
                        public ScheduledExecutorService delayCleanUpExecutor() {
                            return scheduledExecutorService;
                        }

                        @Nonnull
                        @Override
                        public LongSupplier cleanUpDelayMs() {
                            return () -> SECONDS.toMillis(11);
                        }
                    };

                    @Override
                    public DelayCleanUpConfig delayCleanUpConfig() {
                        return delayCleanUpConfig;
                    }
                });
        ConfigAdapter<RefreshAbleService> refreshAbleServiceConfigAdapter =
                closeableResourceConfigAdapter
                        .map(resource -> new RefreshAbleService(Preconditions.checkNotNull(resource)));

        ExecutorService executorService = Executors
                .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4, new ThreadFactoryBuilder().setDaemon(true).build());

        ArrayList<Future<?>> futures = new ArrayList<>();
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 200; i++) {
            futures.add(executorService.submit(() -> {
                while (stopwatch.elapsed(SECONDS) < 10) {
                    Preconditions.checkNotNull(refreshAbleServiceConfigAdapter.get()).doSomeThing();
                }
            }));
        }
        String newValue = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newValue);

        Assertions.assertDoesNotThrow(() -> futures.forEach(future -> {
            try {
                future.get();
            } catch (Throwable t) {
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }));

        executorService.shutdown();
    }

    private static class CloseableResourceWithData extends CloseableResource {
        private final String data;

        CloseableResourceWithData(String data) {
            this.data = data;
        }

        String getData() {
            return data;
        }
    }

    @Order(11)
    @Test
    void testMultiThreadEventSource() {
        ExecutorService executorService = Executors
                .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4, new ThreadFactoryBuilder().setDaemon(true).build());

        final Map<String, CloseableResourceWithData> createdMap = Maps.newConcurrentMap();
        final long delayCleanUpMs = 1000L;

        AtomicBoolean shutdown = new AtomicBoolean(false);
        AtomicReference<String> dataSource = new AtomicReference<>();
        BaseEventSource eventSource = new BaseEventSource();

        ConfigAdapter<CloseableResourceWithData> configAdapter =
                ConfigAdapters.createMapOnEachUpdate(eventSource, dataSource::get, s -> {
                        })
                        .map(new ConfigAdapterMapper<String, CloseableResourceWithData>() {
                            @Nullable
                            @Override
                            public CloseableResourceWithData map(@Nullable String s) {
                                if (s != null) {
                                    CloseableResourceWithData resource = new CloseableResourceWithData(s);
                                    createdMap.put(s, resource);
                                    return resource;
                                } else {
                                    return null;
                                }
                            }

                            @Override
                            public void cleanUp(@Nullable CloseableResourceWithData closeableResourceWithData) {
                                if (closeableResourceWithData != null) {
                                    closeableResourceWithData.close();
                                }
                            }

                            private final DelayCleanUpConfig delayCleanUpConfig = new DelayCleanUpConfig() {
                                private final ScheduledExecutorService scheduledExecutor =
                                        Executors.newSingleThreadScheduledExecutor();

                                @Nonnull
                                @Override
                                public ScheduledExecutorService delayCleanUpExecutor() {
                                    return scheduledExecutor;
                                }

                                @Nonnull
                                @Override
                                public LongSupplier cleanUpDelayMs() {
                                    return () -> delayCleanUpMs;
                                }
                            };

                            @Override
                            public DelayCleanUpConfig delayCleanUpConfig() {
                                return delayCleanUpConfig;
                            }
                        });

        configAdapter.get();

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            futures.add(executorService.submit(() -> {
                while (!shutdown.get()) {
                    String newData = RandomStringUtils.randomAlphabetic(10);
                    String oldData = dataSource.getAndSet(newData);
                    eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
                    CloseableResourceWithData currentObj = configAdapter.get();
                    Assertions.assertNotNull(currentObj);
                    Assertions.assertFalse(currentObj.isClosed());
                }
            }));
        }

        sleepUninterruptibly(5, SECONDS);
        shutdown.set(true);

        Assertions.assertDoesNotThrow(() -> futures.forEach(f -> {
            try {
                f.get();
            } catch (Throwable t) {
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }));

        dataSource.set(null);
        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        // gc 触发 cleanup
        System.gc();
        sleepUninterruptibly(2 * delayCleanUpMs, MILLISECONDS);
        for (int i = 0; i < 10; i++) {
            try {
                createdMap.forEach((str, resource) -> Assertions.assertTrue(resource.isClosed()));
                break;
            } catch (Throwable t) {
                Uninterruptibles.sleepUninterruptibly(1, SECONDS);
            }
        }
        Assertions.assertNull(configAdapter.get());

        executorService.shutdown();
    }

    @Order(12)
    @Test
    void testDetectCycle1() {
        BaseEventSource eventSource = new BaseEventSource();
        AtomicReference<String> data = new AtomicReference<>();

        AtomicInteger counter = new AtomicInteger(0);

        data.set(RandomStringUtils.randomAlphabetic(10));

        ConfigAdapter<String> configAdapter = ConfigAdapters.createMapOnEachUpdate(eventSource, () -> {
            counter.incrementAndGet();
            return data.get();
        }, s -> {
        });

        ConfigAdapter<String> configAdapter1 = configAdapter.map(s -> {
            eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
            return s;
        });
        System.out.println(configAdapter1.get());

        sleepUninterruptibly(1, SECONDS);

        System.out.println(counter.get());
        Assertions.assertEquals(2, counter.get());
    }

    @Order(13)
    @Test
    void testDetectCycle2() {
        BaseEventSource eventSource = new BaseEventSource();
        AtomicReference<String> data = new AtomicReference<>();

        AtomicInteger counter = new AtomicInteger(0);

        data.set(RandomStringUtils.randomAlphabetic(10));

        ConfigAdapter<String> configAdapter = ConfigAdapters.createMapOnEachUpdate(eventSource, () -> {
            counter.incrementAndGet();
            return data.get();
        }, s -> {
        });

        ConfigAdapter<Integer> configAdapter2 =
                configAdapter.map(s -> Optional.ofNullable(s).map(String::length).orElse(null));
        configAdapter2.addEventListener(eventSource::dispatchEvent);
        System.out.println(configAdapter2.get());
        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        System.out.println(counter.get());
        Assertions.assertEquals(2, counter.get());
    }

    private static class CallTimeCounter2 {

        private static final AtomicInteger cleanUpCallNum = new AtomicInteger();
        private static final AtomicInteger constructorCallNum = new AtomicInteger();

        private volatile boolean closed = false;
        private final String data;

        CallTimeCounter2() {
            this(null);
        }

        CallTimeCounter2(String data) {
            this.data = data;
            constructorCallNum.incrementAndGet();
        }

        public String getData() {
            return data;
        }

        void close() {
            closed = true;
            cleanUpCallNum.incrementAndGet();
        }

        boolean isClosed() {
            return closed;
        }

        static int getConstructorCallNum() {
            return constructorCallNum.get();
        }

        static int getCleanUpCallNum() {
            return cleanUpCallNum.get();
        }

        static void reset() {
            cleanUpCallNum.set(0);
            constructorCallNum.set(0);
        }
    }

    @Order(14)
    @Test
    void testOnlyMapOnUpstreamChangeMapper() {

        AtomicReference<String> data = new AtomicReference<>("");

        ConfigAdapter<CallTimeCounter2> mappedConfig = zkConfigAdapter
                .map(c -> data.get())
                .map(new ConfigAdapterMapper<String, CallTimeCounter2>() {
                    @Override
                    public CallTimeCounter2 map(@Nullable String s) {
                        return new CallTimeCounter2(s);
                    }

                    @Override
                    public void cleanUp(@Nullable CallTimeCounter2 cleanUpCounter) {
                        if (cleanUpCounter != null) {
                            cleanUpCounter.close();
                        }
                    }
                });

        AtomicInteger mapper2CallTimeCounter = new AtomicInteger(0);
        AtomicInteger cleanUp2CallTimeCounter = new AtomicInteger(0);

        ConfigAdapter<String> mappedConfig2 = ConfigAdapters.toMapOnEachUpdateConfigAdapter(mappedConfig)
                .map(new ConfigAdapterMapper<CallTimeCounter2, String>() {
                    @Nullable
                    @Override
                    public String map(@Nullable CallTimeCounter2 callTimeCounter2) {
                        if (callTimeCounter2 != null) {
                            mapper2CallTimeCounter.incrementAndGet();
                            return String.valueOf(callTimeCounter2.hashCode());
                        } else {
                            return null;
                        }
                    }

                    @Override
                    public void cleanUp(@Nullable String s) {
                        if (s != null) {
                            cleanUp2CallTimeCounter.incrementAndGet();
                        }
                    }
                });

        Assertions.assertEquals(0, CallTimeCounter2.getConstructorCallNum());
        Assertions.assertEquals(0, CallTimeCounter2.getCleanUpCallNum());
        Assertions.assertEquals(0, mapper2CallTimeCounter.get());
        Assertions.assertEquals(0, cleanUp2CallTimeCounter.get());

        String newZkValue = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newZkValue);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(0, CallTimeCounter2.getConstructorCallNum());
        Assertions.assertEquals(0, CallTimeCounter2.getCleanUpCallNum());
        Assertions.assertEquals(0, mapper2CallTimeCounter.get());
        Assertions.assertEquals(0, cleanUp2CallTimeCounter.get());

        mappedConfig2.get();

        sleepUninterruptibly(1, TimeUnit.SECONDS);

        Assertions.assertFalse(Objects.requireNonNull(mappedConfig.get()).isClosed());
        Assertions.assertEquals("", Objects.requireNonNull(mappedConfig.get()).getData());

        Assertions.assertEquals(1, CallTimeCounter2.getConstructorCallNum());
        Assertions.assertEquals(0, CallTimeCounter2.getCleanUpCallNum());
        Assertions.assertEquals(2, mapper2CallTimeCounter.get());
        Assertions.assertEquals(1, cleanUp2CallTimeCounter.get());

        String newZkValue2 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newZkValue2);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(1, CallTimeCounter2.getConstructorCallNum());
        Assertions.assertEquals(0, CallTimeCounter2.getCleanUpCallNum());
        Assertions.assertEquals(3, mapper2CallTimeCounter.get());
        Assertions.assertEquals(2, cleanUp2CallTimeCounter.get());

        Assertions.assertFalse(Objects.requireNonNull(mappedConfig.get()).isClosed());
        Assertions.assertEquals("", Objects.requireNonNull(mappedConfig.get()).getData());

        data.set("newValue");
        String newZkValue3 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newZkValue3);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(2, CallTimeCounter2.getConstructorCallNum());
        Assertions.assertEquals(1, CallTimeCounter2.getCleanUpCallNum());
        Assertions.assertEquals(4, mapper2CallTimeCounter.get());
        Assertions.assertEquals(3, cleanUp2CallTimeCounter.get());

        Assertions.assertFalse(Objects.requireNonNull(mappedConfig.get()).isClosed());
        Assertions.assertEquals("newValue", Objects.requireNonNull(mappedConfig.get()).getData());
    }

    @Order(15)
    @Test
    void testMapWithLastValueMapper() {

        AtomicReference<String> lastFromValue = new AtomicReference<>();
        AtomicReference<List<String>> lastToValue = new AtomicReference<>();

        ConfigAdapter<List<String>> configAdapter = zkConfigAdapter
                .map(MapWithLastValueMapper.wrap((lastFrom, lastTo, newFrom) -> {
                    lastFromValue.set(lastFrom);
                    lastToValue.set(lastTo);
                    return Optional.ofNullable(lastTo).map(v -> ImmutableList.<String>builder()
                                    .addAll(lastTo)
                                    .add(Optional.ofNullable(newFrom)
                                            .orElse("null")).
                                    build())
                            .orElse(ImmutableList.of(Optional.ofNullable(newFrom)
                                    .orElse("null")));
                }));

        setZk(ZK_KEY_PATH, "{}");
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertNull(lastFromValue.get());
        Assertions.assertNull(lastToValue.get());
        List<String> currentValue = configAdapter.get();
        Assertions.assertNotNull(currentValue);
        Assertions.assertArrayEquals(new String[] {"{}"}, currentValue.toArray());
        Assertions.assertNull(lastFromValue.get());
        Assertions.assertNull(lastToValue.get());

        String newValue1 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newValue1);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals("{}", lastFromValue.get());
        Assertions.assertArrayEquals(new String[] {"{}"}, lastToValue.get().toArray());
        currentValue = configAdapter.get();
        Assertions.assertNotNull(currentValue);
        Assertions.assertArrayEquals(new String[] {"{}", newValue1}, currentValue.toArray());

        String newValue2 = RandomStringUtils.randomAlphabetic(10);
        setZk(ZK_KEY_PATH, newValue2);
        sleepUninterruptibly(ZK_DELAY_S, TimeUnit.SECONDS);

        Assertions.assertEquals(newValue1, lastFromValue.get());
        Assertions.assertArrayEquals(new String[] {"{}", newValue1}, lastToValue.get().toArray());
        currentValue = configAdapter.get();
        Assertions.assertNotNull(currentValue);
        Assertions.assertArrayEquals(currentValue.toArray(), new String[] {"{}", newValue1, newValue2});
    }

    @Order(16)
    @Test
    void testIsolateInfinityBlock() {

        AtomicBoolean blockInfinity = new AtomicBoolean(false);

        ConfigAdapter<String> blocker = SIMPLE_CONFIG_ADAPTER.map(s -> {
            if (blockInfinity.get()) {
                // sleep 这么久大概够跑完这个测试用例了
                sleepUninterruptibly(20, TimeUnit.SECONDS);
            }
            return s;
        });

        AtomicReference<String> someConfigSource = new AtomicReference<>(null);
        ConfigAdapter<String> configAdapter3 =
                SupplierConfigAdapter.createConfigAdapter(someConfigSource::get);

        String value1 = "11111";
        someConfigSource.set(value1);
        configSource.set(value1);
        sleepUninterruptibly(2, TimeUnit.SECONDS);

        Assertions.assertEquals(value1, configAdapter3.get());
        Assertions.assertEquals(value1, SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals(value1, blocker.get());

        blockInfinity.set(true);

        String value2 = "22222";
        someConfigSource.set(value2);
        configSource.set(value2);
        sleepUninterruptibly(2, TimeUnit.SECONDS);

        Assertions.assertEquals(value2, SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals(value2, configAdapter3.get());
        Assertions.assertEquals(value1, blocker.get());

        String value3 = "33333";
        someConfigSource.set(value3);
        configSource.set(value3);
        sleepUninterruptibly(2, TimeUnit.SECONDS);

        Assertions.assertEquals(value3, configAdapter3.get());
        Assertions.assertEquals(value2, SIMPLE_CONFIG_ADAPTER.get());
        Assertions.assertEquals(value1, blocker.get());
    }

    static class Pojo1 {
        private final String data;

        Pojo1(String data) {
            this.data = data;
        }
    }

    @Order(17)
    @Test
    void testUpdateWithRetryMapper() {
        RetryConfig retryConfig = new RetryForever(2, 1000, 120_000, null);
        String errorConfig = "2";
        AtomicInteger retryTimes = new AtomicInteger(0);
        setZk(ZK_KEY_PATH, "0");

        ConfigAdapter<String> mappedConfig = zkConfigAdapter.map(UpdateWithRetryMapper.wrap(s -> {
            if (errorConfig.equals(s)) {
                retryTimes.incrementAndGet();
                throw new RuntimeException();
            }
            return s;
        }, retryConfig));

        // 初始化
        mappedConfig.get();

        setZk(ZK_KEY_PATH, "1");
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);
        Assertions.assertEquals("1", mappedConfig.get());

        setZk(ZK_KEY_PATH, "0");
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);
        Assertions.assertEquals("0", mappedConfig.get());

        setZk(ZK_KEY_PATH, errorConfig);
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        Assertions.assertEquals("0", mappedConfig.get());
        Assertions.assertEquals(1, retryTimes.get());

        sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals(2, retryTimes.get());

        sleepUninterruptibly(4100, MILLISECONDS);
        Assertions.assertEquals(3, retryTimes.get());

        setZk(ZK_KEY_PATH, "3");
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        Assertions.assertEquals("3", mappedConfig.get());
        Assertions.assertEquals(3, retryTimes.get());

        sleepUninterruptibly(1000, MILLISECONDS);
        Assertions.assertEquals(3, retryTimes.get());
    }

    @Order(18)
    @Test
    void testUpdateWithRetryMapper2() {
        RetryConfig retryConfig = new RetryForever(2, 1000, 120_000, null);
        String errorConfig = "2";
        AtomicInteger retryTimes = new AtomicInteger(0);
        AtomicBoolean shouldThrow = new AtomicBoolean(true);

        ConfigAdapter<String> mappedConfig = zkConfigAdapter.map(UpdateWithRetryMapper.wrap(s -> {
            retryTimes.incrementAndGet();
            if (errorConfig.equals(s) && shouldThrow.get()) {
                throw new RuntimeException();
            }
            return s;
        }, retryConfig));

        setZk(ZK_KEY_PATH, "1");
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        Assertions.assertEquals("1", mappedConfig.get());

        setZk(ZK_KEY_PATH, errorConfig);
        sleepUninterruptibly(ZK_DELAY_S, SECONDS);

        sleepUninterruptibly(500, MILLISECONDS);
        Assertions.assertEquals("1", mappedConfig.get());
        Assertions.assertEquals(2, retryTimes.get());

        sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals("1", mappedConfig.get());
        Assertions.assertEquals(3, retryTimes.get());

        shouldThrow.set(false);
        sleepUninterruptibly(4100, MILLISECONDS);
        Assertions.assertEquals("2", mappedConfig.get());
        Assertions.assertEquals(4, retryTimes.get());

        sleepUninterruptibly(8200, MILLISECONDS);
        Assertions.assertEquals("2", mappedConfig.get());
        Assertions.assertEquals(4, retryTimes.get());
    }

    private static class TestResource implements AutoCloseable {
        private static final AtomicLong CREATE_COUNTER = new AtomicLong(0);
        private static final AtomicLong CLOSE_COUNTER = new AtomicLong(0);

        public TestResource() {
            CREATE_COUNTER.incrementAndGet();
        }

        @Override
        public void close() {
            CLOSE_COUNTER.incrementAndGet();
        }
    }

    @Test
    void testClose() throws Throwable {
        AtomicReference<String> configSource = new AtomicReference<>(null);
        BaseEventSource eventSource = new BaseEventSource();
        ConfigAdapter<TestResource> configAdapter = ConfigAdapters.create(eventSource, configSource::get, s -> {
                })
                .map(new ConfigAdapterMapper<String, TestResource>() {
                    @Override
                    public TestResource map(@Nullable String s) {
                        return new TestResource();
                    }

                    @Override
                    public void cleanUp(@Nullable TestResource testResource) {
                        if (testResource != null) {
                            testResource.close();
                        }
                    }
                });
        Assertions.assertEquals(0, TestResource.CREATE_COUNTER.get());
        Assertions.assertEquals(0, TestResource.CLOSE_COUNTER.get());
        configAdapter.get();
        Assertions.assertEquals(1, TestResource.CREATE_COUNTER.get());
        Assertions.assertEquals(0, TestResource.CLOSE_COUNTER.get());
        configSource.set(RandomStringUtils.randomAlphabetic(10));
        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        Assertions.assertEquals(2, TestResource.CREATE_COUNTER.get());
        Assertions.assertEquals(1, TestResource.CLOSE_COUNTER.get());
        configAdapter.close();
        Assertions.assertEquals(2, TestResource.CREATE_COUNTER.get());
        Assertions.assertEquals(2, TestResource.CLOSE_COUNTER.get());
        configAdapter.close();
        Assertions.assertEquals(2, TestResource.CREATE_COUNTER.get());
        Assertions.assertEquals(2, TestResource.CLOSE_COUNTER.get());
        configSource.set(RandomStringUtils.randomAlphabetic(10));
        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        Assertions.assertEquals(2, TestResource.CREATE_COUNTER.get());
        Assertions.assertEquals(2, TestResource.CLOSE_COUNTER.get());
        Assertions.assertThrows(RuntimeException.class, configAdapter::get);
        Assertions.assertThrows(RuntimeException.class, () -> configAdapter.map(s -> s));
        Assertions.assertThrows(RuntimeException.class, () -> configAdapter.addEventListener(event -> {
        }));
        Assertions.assertThrows(RuntimeException.class, () -> configAdapter.removeEventListener(event -> {
        }));
    }
}