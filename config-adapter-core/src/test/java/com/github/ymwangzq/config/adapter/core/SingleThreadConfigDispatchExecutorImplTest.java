package com.github.ymwangzq.config.adapter.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author myco
 * Created on 2021-05-29
 */
class SingleThreadConfigDispatchExecutorImplTest {

    @Test
    void testExecute() throws Throwable {
        AtomicInteger counter = new AtomicInteger(0);
        SingleThreadConfigDispatchExecutorImpl configDispatchExecutor =
                new SingleThreadConfigDispatchExecutorImpl(new ThreadFactoryBuilder()
                        .setNameFormat("ConfigDispatchExecutorImplTest-testExecute-%d")
                        // 验证下每次都创建新线程是否有问题
                        .build(), 0, TimeUnit.NANOSECONDS);

        int targetValue = 100000;
        AtomicInteger cancelCounter = new AtomicInteger(0);
        AtomicInteger finalValue = new AtomicInteger();
        AtomicInteger interruptCounter = new AtomicInteger(0);

        List<Future<Integer>> futures = new ArrayList<>(targetValue);

        ListenableFuture<Integer> future = null;
        for (int i = 0; i < targetValue; i++) {
            int i1 = i + 1;
            future = configDispatchExecutor.interruptCurrentAndSubmit(() -> {
                counter.incrementAndGet();
                finalValue.set(i1);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        interruptCounter.incrementAndGet();
                    }
                    throw t;
                }
                return i1;
            });
            futures.add(future);
            Futures.addCallback(future, new FutureCallback<Integer>() {
                @Override
                public void onSuccess(@Nullable Integer result) {
                    // ignore
                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof CancellationException) {
                        cancelCounter.incrementAndGet();
                    }
                }
            }, MoreExecutors.directExecutor());
        }

        try {
            future.get(1100, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
//            t.printStackTrace();
        }
        for (int i = 0; i < futures.size(); i++) {
            Assertions.assertTrue(futures.get(i).isDone(), "" + i);
        }
        System.out.println(
                "targetValue: " + targetValue + " conter: " + counter.get() + " cancelCounter: " + cancelCounter.get()
                        + " sum: " + (counter.get() + cancelCounter.get()));
        System.out.println("interruptCounter: " + interruptCounter.get());
        Assertions.assertEquals(targetValue, counter.get() + cancelCounter.get());
        Assertions.assertEquals(targetValue, finalValue.get());
        Assertions.assertEquals(targetValue, future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void testInterrupt() throws Throwable {
        long loop = 100000L;

        SingleThreadConfigDispatchExecutorImpl configDispatchExecutor =
                new SingleThreadConfigDispatchExecutorImpl(new ThreadFactoryBuilder()
                        .setNameFormat("ConfigDispatchExecutorImplTest-testExecute-%d")
                        // 验证下每次都创建新线程是否有问题
                        .build(), 0, TimeUnit.NANOSECONDS);

        AtomicLong errorInterruptCounter = new AtomicLong(0);
        AtomicLong normalInterruptCounter = new AtomicLong(0);
        AtomicLong cancelCounter = new AtomicLong(0);
        AtomicLong finishCounter = new AtomicLong(0);

        ListenableFuture<Void> future = null;
        for (int i = 0; i < loop; i++) {
            ListenableFuture<Void> lastFuture = future;
            future = configDispatchExecutor.interruptCurrentAndSubmit(() -> {
                // 这里也有概率被 interrupt，数量不要太多就好了
                if (Thread.currentThread().isInterrupted()) {
                    errorInterruptCounter.incrementAndGet();
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException t) {
                    normalInterruptCounter.incrementAndGet();
                    throw t;
                }
                finishCounter.incrementAndGet();
                return null;
            });
            if (lastFuture != null) {
                try {
                    lastFuture.get(1010, TimeUnit.MILLISECONDS);
                } catch (Throwable t) {
                    Throwable rootCause = Throwables.getRootCause(t);
                    if (rootCause instanceof CancellationException) {
                        cancelCounter.incrementAndGet();
                    }
                    boolean condition =
                            rootCause instanceof CancellationException || rootCause instanceof InterruptedException;
                    Assertions.assertTrue(condition, "i = " + i + " " + Throwables.getStackTraceAsString(t));
                }
            }
        }
        future.get(2, TimeUnit.SECONDS);

        System.out.println("errorInterruptCounter: " + errorInterruptCounter.get() + " normalInterruptCounter: "
                + normalInterruptCounter.get() + " cancelCounter: " + cancelCounter.get() + " finishCounter: "
                + finishCounter.get());

        Assertions.assertTrue(errorInterruptCounter.get() <= 100, "" + errorInterruptCounter.get());
        Assertions.assertTrue(finishCounter.get() > 0, "" + finishCounter.get());
        Assertions.assertTrue(finishCounter.get() < 10, "" + finishCounter.get());
        Assertions.assertEquals(loop, normalInterruptCounter.get() + cancelCounter.get() + finishCounter.get());
    }
}