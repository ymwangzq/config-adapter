package com.github.ymwangzq.config.adapter.core;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.github.ymwangzq.config.adapter.core.GcUtil.doBatchFinalize;
import static com.github.ymwangzq.config.adapter.core.GcUtil.getFinalizerMapSize;
import static com.github.ymwangzq.config.adapter.core.SupplierConfigAdapter.getSupplierChangeHelpersSize;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2020-12-10
 */
@SuppressWarnings("UnusedAssignment")
class ConfigAdaptersTest {

    @BeforeAll
    static void beforeAll() {
        System.setProperty("SupplierConfigAdapter.check.interval.ms", "10");
    }

    @Test
    void testFlatMap() {
        Map<String, String> configSource = Maps.newConcurrentMap();

        String key1 = "k1";
        String key2 = "k2";
        String key3 = "k3";

        ConfigAdapter<String> ca1 = SupplierConfigAdapter.createConfigAdapter(() -> configSource.get(key1));
        Assertions.assertNull(ca1.get());

        configSource.put(key1, "v1.1");
        configSource.put(key2, "v2.1");
        configSource.put(key3, "v3.1");

        Uninterruptibles.sleepUninterruptibly(1, SECONDS);
        Assertions.assertEquals("v1.1", ca1.get());

        ConfigAdapter<String> ca = ConfigAdapters.flatMap(ca1, v -> {
            if (StringUtils.startsWith(v, "k")) {
                return SupplierConfigAdapter.createConfigAdapter(() -> configSource.get(v));
            } else {
                return SupplierConfigAdapter.createConfigAdapter(() -> v);
            }
        });

        Assertions.assertEquals("v1.1", ca.get());

        configSource.put(key1, key2);

        Uninterruptibles.sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals("v2.1", ca.get());
        configSource.put(key2, "v2.2");
        Uninterruptibles.sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals("v2.2", ca.get());

        configSource.put(key1, key1);
        Uninterruptibles.sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals(key1, ca.get());

        configSource.put(key1, key3);
        Uninterruptibles.sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals("v3.1", ca.get());
        configSource.put(key2, "v2.2");
        Uninterruptibles.sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals("v3.1", ca.get());
        configSource.put(key3, "v3.2");
        Uninterruptibles.sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals("v3.2", ca.get());

        configSource.put(key1, key1);
        Uninterruptibles.sleepUninterruptibly(2100, MILLISECONDS);
        Assertions.assertEquals(key1, ca.get());

        ca = null;
        ca1 = null;

        for (int i = 0; i < 20 && getFinalizerMapSize() > 0; i++) {
            doBatchFinalize();
            System.gc();
            System.out.println("done: " + getFinalizerMapSize());
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        Assertions.assertEquals(0, getFinalizerMapSize());
    }

    @Test
    void testMemoryLeak() {
        Map<String, String> configSource = Maps.newConcurrentMap();

        String key1 = "k1";
        String key2 = "k2";
        String key3 = "k3";

        ConfigAdapter<String> ca1 = SupplierConfigAdapter.createConfigAdapter(() -> configSource.get(key1));
        Assertions.assertNull(ca1.get());

        configSource.put(key1, "v1.1");
        configSource.put(key2, "v2.1");
        configSource.put(key3, "v3.1");

        List<ConfigAdapter<String>> cas = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ConfigAdapter<String> ca = ConfigAdapters.flatMap(ca1, v -> {
                if (StringUtils.startsWith(v, "k")) {
                    return SupplierConfigAdapter.createConfigAdapter(() -> configSource.get(v));
                } else {
                    return SupplierConfigAdapter.createConfigAdapter(() -> v);
                }
            });
            ca.get();
            cas.add(ca);
        }

        for (int i = 0; i < 10; i++) {
            System.out.println("i = " + i);
            configSource.put(key1, key2);
            sleepUninterruptibly(1000, MILLISECONDS);
            for (ConfigAdapter<String> ca : cas) {
                boolean succ = false;
                while (!succ) {
                    try {
                        Assertions.assertEquals("v2.1", ca.get(), "i = " + i);
                        succ = true;
                    } catch (Throwable t) {
//                        t.printStackTrace();
                        sleepUninterruptibly(50, MILLISECONDS);
                    }
                }
            }

            configSource.put(key1, key3);
            sleepUninterruptibly(1000, MILLISECONDS);
            for (ConfigAdapter<String> ca : cas) {
                boolean succ = false;
                while (!succ) {
                    try {
                        Assertions.assertEquals("v3.1", ca.get(), "i = " + i);
                        succ = true;
                    } catch (Throwable t) {
//                        t.printStackTrace();
                        sleepUninterruptibly(50, MILLISECONDS);
                    }
                }
            }

            configSource.put(key1, key1);
            sleepUninterruptibly(1000, MILLISECONDS);
            for (ConfigAdapter<String> ca : cas) {
                boolean succ = false;
                while (!succ) {
                    try {
                        Assertions.assertEquals("k1", ca.get(), "i = " + i);
                        succ = true;
                    } catch (Throwable t) {
//                        t.printStackTrace();
                        sleepUninterruptibly(50, MILLISECONDS);
                    }
                }
            }

            configSource.put(key1, "v1.1");
            sleepUninterruptibly(1000, MILLISECONDS);
            for (ConfigAdapter<String> c : cas) {
                boolean succ = false;
                while (!succ) {
                    try {
                        Assertions.assertEquals("v1.1", c.get(), "i = " + i);
                        succ = true;
                    } catch (Throwable t) {
//                        t.printStackTrace();
                        sleepUninterruptibly(50, MILLISECONDS);
                    }
                }
            }
        }

        cas.clear();
        ca1 = null;

        for (int i = 0; i < 20 && getFinalizerMapSize() > 0; i++) {
            doBatchFinalize();
            System.gc();
            System.out.println("done: " + getFinalizerMapSize());
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        Assertions.assertEquals(0, getFinalizerMapSize());
    }

    @Test
    void testMemoryLeak2() {
        Map<String, String> configSource = Maps.newConcurrentMap();

        String key1 = "k1";
        String key2 = "k2";
        String key3 = "k3";

        ConfigAdapter<String> ca1 = SupplierConfigAdapter.createConfigAdapter(() -> configSource.get(key1));
        Assertions.assertNull(ca1.get());

        configSource.put(key1, "v1.1");
        configSource.put(key2, "v2.1");
        configSource.put(key3, "v3.1");

        List<ConfigAdapter<String>> cas = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            ConfigAdapter<String> ca = ca1.map(s -> s + s);
            ca.get();
            cas.add(ca);
        }

        configSource.put(key1, key2);
        sleepUninterruptibly(1, SECONDS);
        cas.forEach(c -> Assertions.assertEquals("k2k2", c.get()));

        cas.clear();
        ca1 = null;

        for (int i = 0; i < 20 && getFinalizerMapSize() > 0; i++) {
            doBatchFinalize();
            System.gc();
            System.out.println("done: " + getFinalizerMapSize());
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        Assertions.assertEquals(0, getFinalizerMapSize());
    }

    @Test
    void testAllAsList() {
        Map<String, String> map = Maps.newConcurrentMap();
        Set<String> keys = Sets.newConcurrentHashSet();

        ConfigAdapter<Set<String>> keysConfig =
                SupplierConfigAdapter.createConfigAdapter(() -> ImmutableSet.copyOf(keys)).map(ImmutableSet::copyOf);
        ConfigAdapter<List<String>> valuesConfig = ConfigAdapters.flatMap(keysConfig, ks -> {
            Set<ConfigAdapter<String>> valueConfigs = Objects.requireNonNull(ks).stream()
                    .map(k -> SupplierConfigAdapter.createConfigAdapter(() -> map.get(k)))
                    .collect(Collectors.toSet());
            return ConfigAdapters.allAsList(valueConfigs);
        });

        long supplierChangeHelpersSize1 = getSupplierChangeHelpersSize();

        Assertions.assertTrue(keysConfig.get().isEmpty());
        Assertions.assertTrue(valuesConfig.get().isEmpty());

        for (int i = 0; i < 10; i++) {
            keys.add(String.valueOf(i));
        }
        Uninterruptibles.sleepUninterruptibly(3, SECONDS);

        Assertions.assertFalse(keysConfig.get().isEmpty());
        Assertions.assertFalse(valuesConfig.get().isEmpty());

        Assertions.assertTrue(valuesConfig.get().stream().allMatch(Objects::isNull));

        long supplierChangeHelpersSize2 = getSupplierChangeHelpersSize();

        for (int i = 0; i < 10; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
        }
        Uninterruptibles.sleepUninterruptibly(2, SECONDS);
        Assertions.assertTrue(valuesConfig.get().stream().allMatch(Objects::nonNull));

        keys.clear();
        Uninterruptibles.sleepUninterruptibly(2, SECONDS);

        for (int i = 0; i < 10 || getSupplierChangeHelpersSize() > supplierChangeHelpersSize1; i++) {
            System.gc();
            Uninterruptibles.sleepUninterruptibly(1, SECONDS);
        }
        Assertions.assertTrue(supplierChangeHelpersSize1 < supplierChangeHelpersSize2);
        Assertions.assertTrue(getSupplierChangeHelpersSize() <= supplierChangeHelpersSize1,
                "SupplierConfigAdapter.SUPPLIER_CHANGE_HELPERS leak!");
    }
}