package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ATCallback;
import at.feedapi.Helpers;
import at.feedapi.MarketMoversDbResponseCollection;
import at.shared.ATServerAPIDefines;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketDataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CreateMarketMoversCommand extends MarketAPIBaseCommand<List<Map<String, Object>>> {
    private static final Logger logger = LoggerFactory.getLogger(CreateMarketMoversCommand.class);

    public CreateMarketMoversCommand(MarketData marketData) {
        super(marketData);
    }

    @Override
    public List<Map<String, Object>> handle(CommandRequestWrapper request) {
        CreateMarketMoversDbRequest payload = (CreateMarketMoversDbRequest) request;
        logger.info("handle: {}, {}", payload.getSymbol(), payload.getExchangeType());

        List<ATServerAPIDefines.ATSYMBOL> symbols = new ArrayList<>();
        ATServerAPIDefines.ATSYMBOL atsymbol = Helpers.StringToSymbol(payload.getSymbol().name());
        atsymbol.symbolType = ATServerAPIDefines.ATSymbolType.TopMarketMovers;
        atsymbol.exchangeType = payload.exchangeType.name().getBytes()[0];
        symbols.add(atsymbol);

        MarketMoversDbResponseCallback callback = new MarketMoversDbResponseCallback();
        long requestId = marketData.serverAPI.ATCreateMarketMoversDbRequest(
                marketData.session,
                symbols,
                callback
        );

        sendRequest(requestId);

        return callback.getData();
    }

    static class MarketMoversDbResponseCallback extends ATCallback implements ATMarketMoversDbResponseCallback {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<Map<String, Object>> list = new ArrayList<>();

        List<Map<String, Object>> getData() {
            try {
                countDownLatch.await(10, TimeUnit.SECONDS);
                return list;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void process(long requestId, ATServerAPIDefines.ATMarketMoversDbResponseType responseType, MarketMoversDbResponseCollection collection) {
            logger.info("process: {}, {}, {}", requestId, responseType.m_responseType, collection.GetRecords().size());

            if (responseType.m_responseType == ATServerAPIDefines.ATMarketMoversDbResponseType.MarketMoversDbResponseSuccess) {
                for (ATServerAPIDefines.ATMARKET_MOVERS_RECORD record : collection.GetRecords()) {
                    Map<String, Object> kv = new HashMap<>();
                    kv.put("status", record.status.m_atSymbolStatus);
                    kv.put("symbol", new String(record.symbol.symbol).replaceAll("\u0000", ""));

                    List<Map<String, Object>> items = new ArrayList<>();
                    for (ATServerAPIDefines.ATMARKET_MOVERS_ITEM item : record.items) {
                        Map<String, Object> itemValues = new HashMap<>();
//                        itemValues.put("name", new String(item.name));
                        itemValues.put("symbol", new String(item.symbol.symbol).replaceAll("\u0000", ""));
                        itemValues.put("volume", item.volume);
                        itemValues.put("time", MarketDataUtils.systemTimeToTimestamp(item.lastDateTime).getTimeInMillis());
                        itemValues.put("preClose", item.closePrice.price);
                        itemValues.put("price", item.lastPrice.price);
                        items.add(itemValues);
                    }

                    kv.put("items", items);
                    list.add(kv);
                }
            }

            countDownLatch.countDown();
        }
    }

    public enum SymbolType {
        VL, // Top Volume
        NG, // Net Gainers
        NL, // Net Losers
        PG, // Percent Gainers
        PL, // Percent Losers
    }

    public enum ExchangeType {
        A, // Amex
        U, // OTCBB
        N, // NyseEuronext
        Q, // NasdaqOmx
    }

    public static class CreateMarketMoversDbRequest extends CommandRequestWrapper {
        SymbolType symbol;
        ExchangeType exchangeType;

        public SymbolType getSymbol() {
            return symbol;
        }

        public void setSymbol(SymbolType symbol) {
            this.symbol = symbol;
        }

        public ExchangeType getExchangeType() {
            return exchangeType;
        }

        public void setExchangeType(ExchangeType exchangeType) {
            this.exchangeType = exchangeType;
        }
    }
}
