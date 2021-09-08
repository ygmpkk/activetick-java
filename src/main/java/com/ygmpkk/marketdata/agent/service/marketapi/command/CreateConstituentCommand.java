package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ATCallback;
import at.feedapi.ConstituentsListResponseCollection;
import at.shared.ATServerAPIDefines;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CreateConstituentCommand extends MarketAPIBaseCommand<List<String>> {
    private static final Logger logger = LoggerFactory.getLogger(CreateMarketHolidayCommand.class);

    public CreateConstituentCommand(MarketData marketData) {
        super(marketData);
    }

    @Override
    public List<String> handle(CommandRequestWrapper request) {
        CreateConstituentRequest payload = (CreateConstituentRequest) request;
        MarketHolidayCallback callback = new MarketHolidayCallback();

        long requestId = marketData.serverAPI.ATCreateConstituentListRequest(
                marketData.session,
                (new ATServerAPIDefines()).new ATConstituentListType(payload.getConstituentType().value.byteValue()),
                payload.getSymbol().getBytes(),
                callback
        );

        sendRequest(requestId);

        return callback.getData();
    }

    public enum ConstituentType {
        Index(1),
        Sector(2),
        OptionChain(3);

        public final Integer value;

        private ConstituentType(Integer value) {
            this.value = value;
        }

        private static final Map<Integer, ConstituentType> map;

        static {
            map = Arrays.stream(values()).collect(Collectors.toMap(e -> e.value, e -> e));
        }

        public static ConstituentType fromInt(Integer value) {
            return map.get(value);
        }
    }

    public static class CreateConstituentRequest extends CommandRequestWrapper {
        String symbol;
        ConstituentType constituentType;

        public String getSymbol() {
            return symbol;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        public ConstituentType getConstituentType() {
            return constituentType;
        }

        public void setConstituentType(ConstituentType constituentType) {
            this.constituentType = constituentType;
        }
    }

    public static class MarketHolidayCallback extends ATCallback implements ATConstituentsListResponseCallback {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<String> list = new ArrayList<>();

        List<String> getData() {
            try {
                countDownLatch.await(60, TimeUnit.SECONDS);
                return list;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void process(long requestId, ConstituentsListResponseCollection collection) {
            logger.info("process: {}, {}", requestId, collection.GetSymbols().size());
            for (ATServerAPIDefines.ATSYMBOL symbol : collection.GetSymbols()) {
                String optionSymbol = new String(symbol.symbol, StandardCharsets.UTF_8).replaceAll("\u0000", "");
                list.add(optionSymbol);
            }

            countDownLatch.countDown();
        }
    }
}
