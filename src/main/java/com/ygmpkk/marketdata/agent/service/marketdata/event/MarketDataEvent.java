package com.ygmpkk.marketdata.agent.service.marketdata.event;

import org.springframework.context.ApplicationEvent;

public class MarketDataEvent extends ApplicationEvent {
    private final MarketDataEventType eventType;

    public MarketDataEvent(Object source, MarketDataEventType eventType) {
        super(source);

        this.eventType = eventType;
    }

    public MarketDataEventType getEventType() {
        return eventType;
    }
}
