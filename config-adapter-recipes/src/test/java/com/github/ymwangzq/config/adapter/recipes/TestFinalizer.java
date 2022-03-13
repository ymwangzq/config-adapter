package com.github.ymwangzq.config.adapter.recipes;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.base.MoreObjects;

/**
 * @author myco
 * Created on 2019-12-31
 */
public class TestFinalizer {

    private static class ClosableResource {

        private static final ReferenceQueue<ClosableResource> REFERENCE_QUEUE = new ReferenceQueue<>();
        private static final Map<PhantomReference<ClosableResource>, Runnable> FINALIZER_MAP = Maps.newConcurrentMap();

        private static final AtomicLong idGenerator = new AtomicLong(0);

        private static final AtomicLong FINALIZER_COUNTER = new AtomicLong(0);

        /**
         * 这样创建的可以被 gc
         */
        private static ClosableResource newClosableResource() {
            long id = idGenerator.getAndIncrement();
            ClosableResource closableResource = new ClosableResource(id);
            PhantomReference<ClosableResource> phantomReference =
                    new PhantomReference<>(closableResource, REFERENCE_QUEUE);
            FINALIZER_MAP.put(phantomReference, () -> {
                FINALIZER_COUNTER.incrementAndGet();
                System.out.println("finalizing: " + id);
            });
            doFinalize();
            return closableResource;
        }

        /**
         * 这样创建的无法被 gc，因为有隐式引用
         */
        private static ClosableResource newClosableResource2() {
            return new ClosableResource();
        }

        private final long id;

        private ClosableResource(long id) {
            this.id = id;
        }

        private ClosableResource() {
            this.id = idGenerator.getAndIncrement();
            PhantomReference<ClosableResource> phantomReference =
                    new PhantomReference<>(this, REFERENCE_QUEUE);
            // 这个 lambda 里隐式引用了当前的 ClosableResource, 导致 ClosableResource 不能被 gc
            FINALIZER_MAP.put(phantomReference, () -> {
                FINALIZER_COUNTER.incrementAndGet();
                System.out.println("finalizing: " + id);
            });
            doFinalize();
        }

        public long getId() {
            return id;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("id", id)
                    .toString();
        }

        private static void doFinalize() {
            Reference<? extends ClosableResource> ref = REFERENCE_QUEUE.poll();
            while (ref != null) {
                //noinspection SuspiciousMethodCalls
                Runnable finalizer = FINALIZER_MAP.remove(ref);
                if (finalizer != null) {
                    try {
                        finalizer.run();
                    } catch (Throwable t) {
//                        t.printStackTrace();
                    }
                }
                ref = REFERENCE_QUEUE.poll();
            }
        }
    }

    @Test
    void test() throws Throwable {
        ExecutorService executorService = Executors.newCachedThreadPool();
        AtomicBoolean running = new AtomicBoolean(true);
        try {
            BlockingQueue<ClosableResource> queue = new ArrayBlockingQueue<>(10000);

            executorService.submit(() -> {
                while (running.get()) {
                    try {
                        queue.put(ClosableResource.newClosableResource());
                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
                    }
                }
            });
            Thread.sleep(1000);
            executorService.submit(() -> {
                while (running.get()) {
                    queue.clear();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
                    }
                }
            });

            for (int i = 0; i < 20; i++) {
                System.gc();
                Thread.sleep(1000);
            }

            System.out.println(ClosableResource.FINALIZER_COUNTER.get());
            Assertions.assertNotEquals(0, ClosableResource.FINALIZER_COUNTER.get());
        } finally {
            running.set(false);
            executorService.shutdown();
        }
    }

    /**
     * 这个会失败
     */
    @Disabled
    @Test
    void test2() throws Throwable {
        ExecutorService executorService = Executors.newCachedThreadPool();
        AtomicBoolean running = new AtomicBoolean(true);
        try {
            BlockingQueue<ClosableResource> queue = new ArrayBlockingQueue<>(10000);

            executorService.submit(() -> {
                while (running.get()) {
                    try {
                        queue.put(ClosableResource.newClosableResource2());
                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
                    }
                }
            });
            Thread.sleep(1000);
            executorService.submit(() -> {
                while (running.get()) {
                    queue.clear();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException interruptedException) {
//                        interruptedException.printStackTrace();
                    }
                }
            });

            Thread.sleep(20000);

            System.out.println(ClosableResource.FINALIZER_COUNTER.get());
            Assertions.assertNotEquals(0, ClosableResource.FINALIZER_COUNTER.get());
        } finally {
            running.set(false);
            executorService.shutdown();
        }
    }
}
