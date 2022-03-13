package com.github.ymwangzq.config.adapter.core;

import org.junit.jupiter.api.Test;

import com.github.ymwangzq.config.adapter.facade.EventSource;

/**
 * @author myco
 * Created on 2019-12-10
 */
class EventSourceUtilTest {

    @Test
    void test() {
        EventSource eventSource = EventSourceUtil.anyOf();
    }
}