package com.github.ymwangzq.config.adapter.facade;

import javax.annotation.Nonnull;

/**
 * ⚠️注意⚠️：这是一个很容易用错的类，如果不确定自己用的对不对，建议不要使用
 * <p>
 * 这个类用于自适应 cleanUp
 * <p>
 * 在配置变更的时候，什么时候对老值做清理工作，是一个难以确定的事情：因为老的值可能正被业务线程拿到并且还在使用；
 * 虽然 BaseConfigAdapter 里支持配置切换后，等一段时间再清理老值，但是等这个时间仍然不是稳妥的策略
 * <p>
 * 最稳妥的办法是：当我们能确定业务线程完全释放掉对老值的引用，那这时候肯定可以做清理工作了
 * <p>
 * 关键是怎么确定业务释放了对老值的引用呢？很难做到；除非老值已经被 gc 掉了，那业务肯定不会再引用了；
 * 可是如果老值已经被 gc 掉了，那我们还怎么做相应清理呢？确实没办法。但是我们有另一种思路：
 * <p>
 * 业务代码里拿到的，是一个包装对象，而我们的清理方法，清理包装对象内部的代理对象，即：
 * 当 BaseConfigAdapter 发现要清理的老对象是 AdaptiveCleanUpProxy 的时候，会认为需要清理的对象是内部的 getDelegate；
 * 于是就可以等外层老值对象被 gc 掉以后，再对内部的 delegate 对象做清理。
 *
 * ⚠️注意⚠️：实现这个类以后，所有的方法都应当代理到 {@link AdaptiveCleanUpProxy#getDelegate()} 方法上
 *
 * @author myco
 * Created on 2020-08-05
 */
public interface AdaptiveCleanUpProxy<T> {

    /**
     * 获取真正需要 cleanUp 的对象
     */
    @Nonnull
    T getDelegate();
}
