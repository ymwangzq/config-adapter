package com.github.ymwangzq.config.adapter.facade;

import javax.annotation.Nonnull;

/**
 * @author myco
 * Created on 2019-10-25
 */
public interface EventListener {
    /**
     * 严禁处理长时间阻塞操作
     */
    void processEvent(@Nonnull StatefulEvent event);
}
