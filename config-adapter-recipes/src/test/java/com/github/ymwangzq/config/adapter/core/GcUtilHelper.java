package com.github.ymwangzq.config.adapter.core;

/**
 * @author myco
 * Created on 2020-01-02
 */
public class GcUtilHelper {
    public static void addFinalizeListener(Runnable finalizeListener) {
        GcUtil.addFinalizeListener(finalizeListener);
    }

    public static void doBatchFinalize() {
        GcUtil.doBatchFinalize();
    }

    public static int getFinalizerMapSize() {
        return GcUtil.getFinalizerMapSize();
    }
}
