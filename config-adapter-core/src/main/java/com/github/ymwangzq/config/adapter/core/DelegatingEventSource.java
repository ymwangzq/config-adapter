package com.github.ymwangzq.config.adapter.core;

import javax.annotation.Nonnull;

import com.github.ymwangzq.config.adapter.facade.EventListener;
import com.github.ymwangzq.config.adapter.facade.EventSource;

/**
 * @author myco
 * Created on 2020-01-01
 */
public class DelegatingEventSource implements EventSource {

    private final EventSource delegate;

    public DelegatingEventSource(EventSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public void addEventListener(
            @Nonnull EventListener eventListener) {
        delegate.addEventListener(eventListener);
    }

    @Override
    public void removeEventListener(
            @Nonnull EventListener eventListener) {
        delegate.removeEventListener(eventListener);
    }
}
