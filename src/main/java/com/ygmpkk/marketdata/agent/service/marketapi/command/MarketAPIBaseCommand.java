package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ATCallback;
import at.feedapi.ATCallback.ATRequestTimeoutCallback;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;

import java.util.Map;

public abstract class MarketAPIBaseCommand<T> extends ATCallback implements ATRequestTimeoutCallback {
    protected MarketData marketData;

    public MarketAPIBaseCommand(MarketData marketData) {
        this.marketData = marketData;
    }

    public abstract T handle(CommandRequestWrapper request);

    @Override
    public void process(long requestId) {

    }

    protected boolean sendRequest(long requestId) {
        return marketData.serverAPI.ATSendRequest(
                marketData.session,
                requestId,
                3000,
                this
        );
    }
}
