package com.ygmpkk.marketdata.agent.runner;

import com.ygmpkk.marketdata.agent.service.marketdata.MarketDataPublisher;
import com.ygmpkk.marketdata.agent.service.marketdata.event.MarketDataEventType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class ApplicationRunner {
    final MarketDataPublisher publisher;

    @Autowired
    public ApplicationRunner(MarketDataPublisher publisher) {
        this.publisher = publisher;
    }

    @PostConstruct
    public void init() {
        // 用状态机控制ActiveTick行情连接、认证和获取
        publisher.pushEvent(MarketDataEventType.CONNECT);
    }
}
