package com.ygmpkk.marketdata.agent.service.marketdata;

import com.ygmpkk.marketdata.agent.service.marketdata.event.MarketDataEvent;
import com.ygmpkk.marketdata.agent.service.marketdata.event.MarketDataEventType;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class MarketDataPublisher {
    private final ApplicationEventPublisher publisher;

    MarketDataPublisher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    public void pushEvent(MarketDataEventType eventType) {
        publisher.publishEvent(new MarketDataEvent(this, eventType));
    }
}
