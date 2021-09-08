package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ActiveTickServerAPI;
import at.feedapi.Helpers;
import at.shared.ATServerAPIDefines;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateMoversSubscriptionCommand extends MarketAPIBaseCommand<String> {
    private static final Logger logger = LoggerFactory.getLogger(CreateMoversSubscriptionCommand.class);

    public CreateMoversSubscriptionCommand(MarketData marketData) {
        super(marketData);
    }

    @Override
    public String handle(CommandRequestWrapper request) {
        CreateMoversStreamRequest payload = (CreateMoversStreamRequest) request;
        List<ATServerAPIDefines.ATSYMBOL> symbols = new ArrayList<>();
        Arrays.asList(payload.getSymbols()).forEach(item -> symbols.add(Helpers.StringToSymbol(item)));

        ATServerAPIDefines.ATStreamRequestType requestType = (new ATServerAPIDefines()).new ATStreamRequestType();
        requestType.m_streamRequestType = payload.getSubscriptionType().value.byteValue();

        long requestId = marketData.requestor.SendATMarketMoversStreamRequest(
                symbols,
                requestType,
                ActiveTickServerAPI.DEFAULT_REQUEST_TIMEOUT
        );

        return Long.toString(requestId);
    }

    public enum SubscriptionType {
        Subscribe(1),
        Unsubscribe(2),
        SubscribeQuotesOnly(3),
        UnsubscribeQuotesOnly(4),
        SubscribeTradesOnly(5),
        UnsubscribeTradesOnly(6);

        public final Integer value;

        private SubscriptionType(Integer value) {
            this.value = value;
        }

        private static final Map<Integer, SubscriptionType> map;

        static {
            map = Arrays.stream(values()).collect(Collectors.toMap(e -> e.value, e -> e));
        }

        public static SubscriptionType fromInt(Integer value) {
            return map.get(value);
        }
    }

    public static class CreateMoversStreamRequest extends CommandRequestWrapper {
        String[] symbols;
        SubscriptionType subscriptionType;

        public String[] getSymbols() {
            return symbols;
        }

        public void setSymbols(String[] symbols) {
            this.symbols = symbols;
        }

        public SubscriptionType getSubscriptionType() {
            return subscriptionType;
        }

        public void setSubscriptionType(SubscriptionType subscriptionType) {
            this.subscriptionType = subscriptionType;
        }
    }
}
