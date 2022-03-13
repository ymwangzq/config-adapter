package com.github.ymwangzq.config.adapter.facade;


import javax.annotation.Nonnull;

/**
 * @author myco
 * Created on 2019-10-11
 */
public interface EventDispatcher {

    void dispatchEvent(@Nonnull StatefulEvent event);
}
