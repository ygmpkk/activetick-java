package com.ygmpkk.marketdata.agent.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "marketapi")
public class MarketAPIConfiguration {
    private String pubsubBind;
    private String reqrepBind;

    public String getPubsubBind() {
        return pubsubBind;
    }

    public void setPubsubBind(String pubsubBind) {
        this.pubsubBind = pubsubBind;
    }

    public String getReqrepBind() {
        return reqrepBind;
    }

    public void setReqrepBind(String reqrepBind) {
        this.reqrepBind = reqrepBind;
    }
}
