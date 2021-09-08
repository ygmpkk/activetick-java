package com.ygmpkk.marketdata.agent.service.marketapi;

import com.ygmpkk.marketdata.agent.config.MarketAPIConfiguration;
import nanomsg.pubsub.PubSocket;
import nanomsg.pubsub.SubSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MarketAPIPubSub {
    static final Logger logger = LoggerFactory.getLogger(MarketAPIPubSub.class);
    final MarketAPIConfiguration configuration;

    PubSocket pubSocket = new PubSocket();
    SubSocket sub = new SubSocket();

    @Autowired
    public MarketAPIPubSub(MarketAPIConfiguration configuration) {
        this.configuration = configuration;


        pubSocket.bind(configuration.getPubsubBind());
        logger.info("pub bind: {}", configuration.getPubsubBind());
    }

    public void pub(String data) {
        pubSocket.send(data);
    }
}
