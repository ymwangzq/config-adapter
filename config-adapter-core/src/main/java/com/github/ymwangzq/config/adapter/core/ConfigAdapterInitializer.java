package com.github.ymwangzq.config.adapter.core;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.github.ymwangzq.config.adapter.core.BaseConfigAdapter.Stat;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2021-11-22
 */
class ConfigAdapterInitializer {

    private static final Logger logger = LoggerFactory.getLogger(ConfigAdapterInitializer.class);

    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("ConfigAdapterInitializer-%d").build());

    private static final Set<BaseConfigAdapter<?, ?>> CONFIG_ADAPTER_SET = Sets.newConcurrentHashSet();

    private static final Supplier<?> INIT = Suppliers.memoize(() -> {
        SCHEDULER.scheduleWithFixedDelay(() -> {
            Iterator<BaseConfigAdapter<?, ?>> iterator = CONFIG_ADAPTER_SET.iterator();
            while (iterator.hasNext()) {
                BaseConfigAdapter<?, ?> configAdapter = iterator.next();
                Stat stat = configAdapter.getStat();
                switch (stat) {
                    case CREATED:
                        // 还没调用过 get 方法，不做任何处理
                        break;
                    case INITIALIZING:
                        try {
                            configAdapter.get();
                            // 初始化成功就可以删掉了
                            iterator.remove();
                        } catch (Throwable t) {
                            logger.warn("retry init config adapter failed!", t);
                        }
                        break;
                    case ACTIVATED:
                    case DETACHED:
                        // 已经 ACTIVATED / DETACHED 的话，后续也不需要再初始化了
                        iterator.remove();
                        break;
                    default:
                        throw new IllegalStateException("BUG!");
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
        return null;
    });

    /**
     * 首次 get 会对 ConfigAdapter 进行初始化。一旦首次 get 抛出异常，
     * 注册一个任务，定期重试 get，直到成功为止。降低业务自己不断重试的成本
     * ConfigAdapter 初始化成功以后，才能确保后续的变更通知机制是好用的。
     */
    static void registerInitUntilSuccess(@Nonnull ConfigAdapter<?> configAdapter) {
        // 确保初始化
        INIT.get();
        // 传入的对象必须是包装过的 ConfigAdapter
        Preconditions.checkArgument(configAdapter instanceof DelegatingConfigAdapter, "BUG!");

        ConfigAdapter<?> core = configAdapter;
        while (core instanceof DelegatingConfigAdapter) {
            core = ((DelegatingConfigAdapter<?>) core).getDelegate();
        }
        // 最终实现一定都是 BaseConfigAdapter
        Preconditions.checkState(core instanceof BaseConfigAdapter, "BUG!");

        BaseConfigAdapter<?, ?> finalCore = (BaseConfigAdapter<?, ?>) core;
        GcUtil.registerCleanUp(configAdapter, () -> CONFIG_ADAPTER_SET.remove(finalCore));
        CONFIG_ADAPTER_SET.add(finalCore);
    }

    static int initQueueSize() {
        return CONFIG_ADAPTER_SET.size();
    }
}
