package com.github.ymwangzq.config.adapter.recipes;

import static com.github.phantomthief.util.MoreFunctions.catching;
import static com.github.phantomthief.util.MoreFunctions.throwing;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.zookeeper.ZkBasedNodeResource;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.github.ymwangzq.config.adapter.core.ConfigAdapterInitializerHelper;
import com.github.ymwangzq.config.adapter.core.ConfigAdapters;
import com.github.ymwangzq.config.adapter.core.EventListeners;
import com.github.ymwangzq.config.adapter.core.GcUtilHelper;
import com.github.ymwangzq.config.adapter.core.UpdateWithRetryMapper;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.EventListener;

/**
 * @author myco
 * Created on 2019-12-10
 */
class ZkBasedNodeResourceConfigAdapterTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkBasedNodeResourceConfigAdapterTest.class);

    private static final String ZK_KEY_PATH = "/config/test/zk/mycoTest";
    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

    private final ListeningExecutorService zkRefreshExecutor = MoreExecutors.listeningDecorator(newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("zk-refresh-thread")
            .build()));

    @BeforeAll
    static void init() throws Throwable {
        testingServer = new TestingServer();
        testingServer.start();
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(testingServer.getConnectString())
                .connectionTimeoutMs((int) TimeUnit.SECONDS.toMillis(1))
                .sessionTimeoutMs((int) TimeUnit.SECONDS.toMillis(3))
                .retryPolicy(new RetryOneTime(100))
                .build();
        curatorFramework.start();
    }

    @AfterAll
    static void afterAll() throws Throwable {
        testingServer.close();
    }

    @Order(2)
    @Test
    void testInterrupt0() {
        ZkBasedNodeResource<String> resource = ZkBasedNodeResource.<String>newGenericBuilder()
                .withCacheFactory(ZK_KEY_PATH,
                        () -> curatorFramework)
                .withStringFactoryEx(s -> s)
                .asyncRefresh(zkRefreshExecutor)
                .build();

        Thread.currentThread().interrupt();
        try {
            try {
                System.out.println(resource.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }

            try {
                System.out.println(resource.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }
        } finally {
            Thread.interrupted();
        }
    }

    @Order(3)
    @Test
    void testInterrupt1() {
        ConfigAdapter<String> zkConfigAdapter =
                ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                                .withCacheFactory(ZK_KEY_PATH,
                                        () -> curatorFramework)
                                .withStringFactoryEx(s -> s)
                                .asyncRefresh(zkRefreshExecutor)
                        , newSingleThreadExecutor(),
                        5000);

        Thread.currentThread().interrupt();
        try {
            try {
                System.out.println(zkConfigAdapter.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }

            try {
                System.out.println(zkConfigAdapter.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }
        } finally {
            Thread.interrupted();
        }
    }

    @Order(4)
    @Test
    void testInterrupt2() {
        ZkBasedNodeResource<String> resource = ZkBasedNodeResource.<String>newGenericBuilder()
                .withCacheFactory(ZK_KEY_PATH,
                        () -> {
                            CuratorFramework tmp =
                                    CuratorFrameworkFactory.builder().connectString(testingServer.getConnectString()) //
                                            .retryPolicy(new RetryForever(100)) //
                                            .sessionTimeoutMs(60_000) //
                                            .connectionTimeoutMs(1000) //
                                            .canBeReadOnly(true) //
                                            .build();
                            tmp.start();
                            return tmp;
                        })
                .withStringFactoryEx(s -> s)
                .asyncRefresh(zkRefreshExecutor)
                .build();

        Thread.currentThread().interrupt();
        try {
            try {
                System.out.println(resource.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }

            try {
                System.out.println(resource.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }
        } finally {
            Thread.interrupted();
        }
    }

    @Order(5)
    @Test
    void testInterrupt3() throws Throwable {
        ConfigAdapter<String> zkConfigAdapter =
                ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                                .withCacheFactory(ZK_KEY_PATH,
                                        () -> {
                                            CuratorFramework tmp =
                                                    CuratorFrameworkFactory.builder()
                                                            .connectString(testingServer.getConnectString()) //
                                                            .retryPolicy(new RetryForever(100)) //
                                                            .sessionTimeoutMs(60_000) //
                                                            .connectionTimeoutMs(1000) //
                                                            .canBeReadOnly(false) //
                                                            .build();
                                            tmp.start();
                                            return tmp;
                                        })
                                .withStringFactoryEx(s -> s)
                                .asyncRefresh(zkRefreshExecutor)
                        , newSingleThreadExecutor(), 5000);

        Thread.currentThread().interrupt();
        try {
            CuratorFramework tmp = CuratorFrameworkFactory.builder()
                    .connectString(testingServer.getConnectString()) //
                    .retryPolicy(new RetryForever(100)) //
                    .sessionTimeoutMs(60_000) //
                    .connectionTimeoutMs(1000) //
                    .canBeReadOnly(false) //
                    .build();
            tmp.start();
            try {
                tmp.create().orSetData().forPath(ZK_KEY_PATH, "asdf".getBytes());
            } catch (Throwable t) {
//                t.printStackTrace();
            }
            System.out.println(Thread.interrupted());
            System.out.println(Thread.interrupted());
            try {
                tmp.create().orSetData().forPath(ZK_KEY_PATH, "asdf".getBytes());
            } catch (Throwable t) {
//                t.printStackTrace();
            }

            try {
                System.out.println(zkConfigAdapter.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }

            try {
                System.out.println(zkConfigAdapter.get());
            } catch (Throwable t) {
//                t.printStackTrace();
            }
        } finally {
            Thread.interrupted();
        }
    }

    @Order(6)
    @Test
    void testZkBaseNodeResourceConfigFinalizer() throws Throwable {
        CuratorFramework tmp =
                CuratorFrameworkFactory.builder()
                        .connectString(testingServer.getConnectString()) //
                        .retryPolicy(new RetryForever(100)) //
                        .sessionTimeoutMs(60_000) //
                        .connectionTimeoutMs(1000) //
                        .canBeReadOnly(false) //
                        .build();
        tmp.start();
        throwing(() -> tmp.create().orSetData().creatingParentsIfNeeded()
                .forPath(ZK_KEY_PATH, "version0".getBytes()));
        AtomicLong finalizeCounter = new AtomicLong();
        GcUtilHelper.addFinalizeListener(finalizeCounter::incrementAndGet);

        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<ConfigAdapter<String>> tempConfigAdapter1Holder = new AtomicReference<>();
        AtomicReference<ConfigAdapter<String>> tempConfigAdapter2Holder = new AtomicReference<>();
        AtomicReference<ConfigAdapter<String>> tempConfigAdapter3Holder = new AtomicReference<>();
        AtomicReference<ConfigAdapter<String>> tempConfigAdapter4Holder = new AtomicReference<>();
        AtomicReference<ConfigAdapter<String>> tempConfigAdapter5Holder = new AtomicReference<>();

        RateLimiter rateLimiter = RateLimiter.create(1);

        AtomicBoolean stop = new AtomicBoolean(false);

        AtomicLong resourceCounter = new AtomicLong(0);

        Future<?> future0 = executorService.submit(() -> {
            Thread.currentThread().setName("creator-thread");
            while (!stop.get()) {
                ConfigAdapter<String> zkConfigAdapter =
                        ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                                        .withCacheFactory(ZK_KEY_PATH,
                                                () -> tmp)
                                        .withStringFactoryEx(s -> s)
                                        .asyncRefresh(zkRefreshExecutor)
                                , newDirectExecutorService(), 5000);

                ConfigAdapter<String> configAdapter1 = zkConfigAdapter.map(new ConfigAdapterMapper<String, String>() {
                    @Override
                    public String map(@Nullable String s) {
                        resourceCounter.incrementAndGet();
                        return "str1: [" + s + "]";
                    }

                    @Override
                    public void cleanUp(@Nullable String s) {
                        if (s != null) {
                            resourceCounter.decrementAndGet();
                        }
                    }
                });
                ConfigAdapter<String> configAdapter2 = ConfigAdapterTest.SIMPLE_CONFIG_ADAPTER.map(s -> "str2: [" + s + "]");
                ConfigAdapter<String> mergedConfigAdapter =
                        ConfigAdapters.mergeTwoConfigAdapters(configAdapter1, configAdapter2,
                                (f1, f2) -> "str3: [" + f1 + " + " + f2 + "]", s -> {
                                });
                ConfigAdapter<String> configAdapter4 = mergedConfigAdapter.map(
                        new ConfigAdapterMapper<String, String>() {
                            @Override
                            public String map(@Nullable String s) {
                                resourceCounter.incrementAndGet();
                                return "str4: [" + s + "]";
                            }

                            @Override
                            public void cleanUp(@Nullable String s) {
                                if (s != null) {
                                    resourceCounter.decrementAndGet();
                                }
                            }
                        });
                ConfigAdapter<String> configAdapter5 = mergedConfigAdapter.map(s -> {
                    throw new RuntimeException();
                });
                if (rateLimiter.tryAcquire()) {
                    catching(configAdapter5::get);
                    configAdapter4.get();
                    System.gc();
                }
                tempConfigAdapter1Holder.set(configAdapter1);
                tempConfigAdapter2Holder.set(configAdapter2);
                tempConfigAdapter3Holder.set(mergedConfigAdapter);
                tempConfigAdapter4Holder.set(configAdapter4);
                tempConfigAdapter5Holder.set(configAdapter5);

                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            }
        });


        Future<?> future = executorService.submit(() -> {
            Thread.currentThread().setName("checker-thread");
            AtomicInteger eventCounter = new AtomicInteger(0);
            ConfigAdapterTest.configSource.set("configVersion1");
            while (tempConfigAdapter4Holder.get() == null) {
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }
            ConfigAdapter<String> tempConfigAdapter4 = tempConfigAdapter4Holder.get();
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            EventListener eventListener = EventListeners.updateEventListener(eventCounter::incrementAndGet);
            tempConfigAdapter4.addEventListener(eventListener);
            eventListener = null;

            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            Assertions.assertNotEquals(tempConfigAdapter4, tempConfigAdapter4Holder.get());
            System.gc();
            Assertions.assertNotEquals(0, finalizeCounter.get());

            tempConfigAdapter4.get();
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            eventCounter.set(0);
            throwing(() -> tmp.create().orSetData().creatingParentsIfNeeded()
                    .forPath(ZK_KEY_PATH, "version1".getBytes()));
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            Assertions
                    .assertEquals("str4: [str3: [str1: [version1] + str2: [configVersion1]]]", tempConfigAdapter4.get());
            Assertions.assertEquals(1, eventCounter.get());

            throwing(() -> tmp.create().orSetData().forPath(ZK_KEY_PATH, "version2".getBytes()));
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            Assertions
                    .assertEquals("str4: [str3: [str1: [version2] + str2: [configVersion1]]]", tempConfigAdapter4.get());
            Assertions.assertEquals(2, eventCounter.get());

            ConfigAdapterTest.configSource.set("configVersion2");
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            throwing(() -> tmp.create().orSetData().creatingParentsIfNeeded()
                    .forPath(ZK_KEY_PATH, "version1".getBytes()));
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            Assertions
                    .assertEquals("str4: [str3: [str1: [version1] + str2: [configVersion2]]]", tempConfigAdapter4.get());
            Assertions.assertEquals(4, eventCounter.get());

            throwing(() -> tmp.create().orSetData().forPath(ZK_KEY_PATH, "version2".getBytes()));
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            Assertions
                    .assertEquals("str4: [str3: [str1: [version2] + str2: [configVersion2]]]", tempConfigAdapter4.get());
            Assertions.assertEquals(5, eventCounter.get());

            Assertions.assertNotEquals(0, finalizeCounter.get());
            Assertions.assertNotEquals(tempConfigAdapter4, tempConfigAdapter4Holder.get());
        });

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        Assertions.assertDoesNotThrow((ThrowingSupplier<?>) future::get);
        Assertions.assertNotEquals(0, resourceCounter.get());

        System.gc();

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        Assertions.assertNotEquals(0, finalizeCounter.get());

        stop.set(true);
        Assertions.assertDoesNotThrow((ThrowingSupplier<?>) future0::get);

        tempConfigAdapter1Holder.set(null);
        tempConfigAdapter2Holder.set(null);
        tempConfigAdapter3Holder.set(null);
        tempConfigAdapter4Holder.set(null);
        tempConfigAdapter5Holder.set(null);

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        System.gc();

        for (int i = 0; i < 20 && GcUtilHelper.getFinalizerMapSize() > 3; i++) {
            GcUtilHelper.doBatchFinalize();
            System.gc();
            System.out.println("done: " + GcUtilHelper.getFinalizerMapSize());
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        Assertions.assertEquals(0, resourceCounter.get());
        System.out.println("getFinalizerMapSize: " + GcUtilHelper.getFinalizerMapSize());
        // 应当只剩一个 ConfigAdapter 没释放，其中会注册 3 个 finalizer
        int finalizerMapSize = GcUtilHelper.getFinalizerMapSize();
        Assertions.assertTrue(finalizerMapSize <= 3, "finalizerMapSize: " + finalizerMapSize);
    }

    @Order(7)
    @Test
    void testGcClose() {
        CuratorFramework tmp =
                CuratorFrameworkFactory.builder()
                        .connectString(testingServer.getConnectString()) //
                        .retryPolicy(new RetryForever(100)) //
                        .sessionTimeoutMs(60_000) //
                        .connectionTimeoutMs(1000) //
                        .canBeReadOnly(false) //
                        .build();
        tmp.start();
        throwing(() -> tmp.create().orSetData().creatingParentsIfNeeded().forPath(ZK_KEY_PATH, "version0".getBytes()));

        ConfigAdapter<String> zkConfigAdapter =
                ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                                .withCacheFactory(ZK_KEY_PATH,
                                        () -> tmp)
                                .withStringFactoryEx(s -> s)
                                .asyncRefresh(zkRefreshExecutor)
                        , newDirectExecutorService(), 5000);

        ConfigAdapter<?> configAdapter0 = zkConfigAdapter.map(s -> "config0: " + s);

        ConfigAdapter<?> mastFailRetryForeverConfigAdapter = zkConfigAdapter.map(UpdateWithRetryMapper.wrap(
                new ConfigAdapterMapper<String, Object>() {
                    private volatile boolean init = false;

                    @Nullable
                    @Override
                    public Object map(@Nullable String s) {
                        if (!init) {
                            init = true;
                            return "retryFail: " + s;
                        } else {
                            throw new RuntimeException();
                        }
                    }
                }, new UpdateWithRetryMapper.RetryForever(2, 1000, 1000, t -> false)));

        ConfigAdapter<?> configAdapter1 = zkConfigAdapter.map(s -> "config1: " + s);

        catching(mastFailRetryForeverConfigAdapter::get);
        System.out.println(configAdapter1.get());

        Assertions.assertEquals("version0", zkConfigAdapter.get());
        Assertions.assertEquals("config0: version0", configAdapter0.get());
        Assertions.assertEquals("config1: version0", configAdapter1.get());

        throwing(() -> tmp.create().orSetData().forPath(ZK_KEY_PATH, "version1".getBytes()));
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        Assertions.assertEquals("version1", zkConfigAdapter.get());
        // configAdapter0 先更新, 值已经变成新的
        Assertions.assertEquals("config0: version1", configAdapter0.get());
        // configAdapter1 在 mastFailRetryForeverConfigAdapter 后更新，被 block, 还是老值
        Assertions.assertEquals("config1: version0", configAdapter1.get());

        throwing(() -> tmp.create().orSetData().forPath(ZK_KEY_PATH, "version2".getBytes()));
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        // configAdapter0 先更新, 值已经变成新的
        Assertions.assertEquals("config0: version2", configAdapter0.get());
        // configAdapter1 在 mastFailRetryForeverConfigAdapter 后更新，mastFailRetryForeverConfigAdapter
        // 被新的事件中断时，从上游获取到的值还没更新，所以落后一个版本
        Assertions.assertEquals("config1: version1", configAdapter1.get());

        // 释放 mastFailRetryForeverConfigAdapter
        mastFailRetryForeverConfigAdapter = null;

        // 触发 gc 并等待清理任务完成
        System.gc();
        Uninterruptibles.sleepUninterruptibly(11, TimeUnit.SECONDS);

        // detach 方法应当能中断在无限重试的 fetcher，中断后 configAdapter1 就应当能正常更新
        Assertions.assertEquals("config0: version2", configAdapter0.get());
        Assertions.assertEquals("config1: version2", configAdapter1.get());

        throwing(() -> tmp.create().orSetData().forPath(ZK_KEY_PATH, "version3".getBytes()));
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        Assertions.assertEquals("config0: version3", configAdapter0.get());
        Assertions.assertEquals("config1: version3", configAdapter1.get());
    }

    @Order(1)
    @Test
    void testInitConnectError() throws Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        int port = testingServer.getPort();
        testingServer.close();
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        AtomicReference<String> valueHolder = new AtomicReference<>();

        try {
            CuratorFramework tmp =
                    CuratorFrameworkFactory.builder()
                            .connectString(testingServer.getConnectString()) //
                            .retryPolicy(new RetryForever(100)) //
                            .sessionTimeoutMs(60_000) //
                            .connectionTimeoutMs(1000) //
                            .canBeReadOnly(false) //
                            .build();
            tmp.start();

            // 尽可能排除之前 case 对这里的干扰
            for (int i = 0; i < 10; i++) {
                if (ConfigAdapterInitializerHelper.initQueueSize() > 10) {
                    System.gc();
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                } else {
                    break;
                }
            }
            int initQueueSize = ConfigAdapterInitializerHelper.initQueueSize();

            ConfigAdapter<String> zkConfigAdapter =
                    ZkBasedNodeResourceConfigAdapter.create(ZkBasedNodeResource.<String>newGenericBuilder()
                                    .withCacheFactory(ZK_KEY_PATH, () -> tmp)
                                    .withStringFactoryEx(s -> s)
                                    .asyncRefresh(zkRefreshExecutor)
                            , executorService, 5000);
            Assertions.assertEquals(initQueueSize + 1, ConfigAdapterInitializerHelper.initQueueSize());
            ConfigAdapter<String> configAdapter = zkConfigAdapter.map(s -> {
                valueHolder.set(s);
                return s;
            });
            Assertions.assertEquals(initQueueSize + 2, ConfigAdapterInitializerHelper.initQueueSize());

            try {
                configAdapter.get();
                Assertions.fail("应当超时");
            } catch (Throwable t) {
                logger.error("", t);
                Assertions.assertTrue(Throwables.getRootCause(t) instanceof TimeoutException);
            }

            Assertions.assertNull(valueHolder.get());
            testingServer = new TestingServer(port);
            testingServer.start();
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

            throwing(() -> tmp.create().orSetData().creatingParentsIfNeeded()
                    .forPath(ZK_KEY_PATH, "version0".getBytes()));

            // 得等上一轮重试超时以后才可能成功。
            Uninterruptibles.sleepUninterruptibly(12, TimeUnit.SECONDS);
            Assertions.assertEquals(initQueueSize, ConfigAdapterInitializerHelper.initQueueSize());

            Assertions.assertEquals("version0", valueHolder.get());

            throwing(() -> tmp.create().orSetData().creatingParentsIfNeeded()
                    .forPath(ZK_KEY_PATH, "version1".getBytes()));

            Uninterruptibles.sleepUninterruptibly(15, TimeUnit.MILLISECONDS);
            Assertions.assertEquals("version1", valueHolder.get());

            tmp.close();
        } finally {
            executorService.shutdown();
        }
    }
}