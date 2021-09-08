package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ATCallback;
import at.feedapi.Helpers;
import at.feedapi.TickHistoryDbResponseCollection;
import at.shared.ATServerAPIDefines;
import at.shared.ActiveTick.DateTime;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketDataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CreateTickHistoryCommand extends MarketAPIBaseCommand<List<Map<String, Object>>> {
    private static final Logger logger = LoggerFactory.getLogger(CreateTickHistoryCommand.class);

    public CreateTickHistoryCommand(MarketData marketData) {
        super(marketData);
    }

    @Override
    public List<Map<String, Object>> handle(CommandRequestWrapper request) {
        CreateTickHistoryRequest payload = (CreateTickHistoryRequest) request;
        ATServerAPIDefines.ATSYMBOL symbol = Helpers.StringToSymbol(payload.getSymbol());

        logger.info("handle: {}", payload);

        TickHistoryCallback callback = new TickHistoryCallback();
        long requestId;
        if (payload.getBegin() != null && payload.getEnd() != null) {
            logger.info("handle begin and end");
            requestId = marketData.serverAPI.ATCreateTickHistoryDbRequest(
                    marketData.session,
                    symbol,
                    payload.getType() == TickHistoryType.T,
                    payload.getType() == TickHistoryType.Q,
                    dateTimeToSystemTime(payload.getBegin()),
                    dateTimeToSystemTime(payload.getEnd()),
                    callback
            );
        } else if (payload.getBegin() != null && payload.getLimit() != null && payload.getCursorType() != null) {
            logger.info("handle begin and limit and cursorType");
            requestId = marketData.serverAPI.ATCreateTickHistoryDbRequest(
                    marketData.session,
                    symbol,
                    payload.getType() == TickHistoryType.T,
                    payload.getType() == TickHistoryType.Q,
                    dateTimeToSystemTime(payload.getBegin()),
                    payload.getLimit(),
                    (new ATServerAPIDefines()).new ATCursorType(Byte.parseByte(payload.getCursorType().value.toString())),
                    callback
            );
        } else if (payload.getBegin() != null && payload.getLimit() != null) {
            logger.info("handle begin and limit");
            requestId = marketData.serverAPI.ATCreateTickHistoryDbRequest(
                    marketData.session,
                    symbol,
                    true,
                    true,
                    dateTimeToSystemTime(payload.getBegin()),
                    payload.getLimit(),
                    (new ATServerAPIDefines()).new ATCursorType(ATServerAPIDefines.ATCursorType.CursorForward),
                    callback
            );
        } else if (payload.getLimit() != null) {
            logger.info("handle limit");
            requestId = marketData.serverAPI.ATCreateTickHistoryDbRequest(
                    marketData.session,
                    symbol,
                    payload.getType() == TickHistoryType.T,
                    payload.getType() == TickHistoryType.Q,
                    payload.getLimit(),
                    callback
            );
        } else {
            throw new RuntimeException("参数错误");
        }

        logger.info("before requestId: {}", requestId);

        sendRequest(requestId);

        return callback.getData();
    }

    ATServerAPIDefines.SYSTEMTIME dateTimeToSystemTime(Date dateTime) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return Helpers.StringToATTime(dateFormat.format(dateTime));
    }

    public enum TickHistoryType {
        T,
        Q,
    }

    public enum CursorType {
        CursorForward(1),
        CursorBackward(2);

        public final Integer value;

        private CursorType(Integer value) {
            this.value = value;
        }

        private static final Map<Integer, CreateTickHistoryCommand.CursorType> map;

        static {
            map = Arrays.stream(values()).collect(Collectors.toMap(e -> e.value, e -> e));
        }

        public static CreateTickHistoryCommand.CursorType fromInt(Integer value) {
            return map.get(value);
        }
    }

    public static class CreateTickHistoryRequest extends CommandRequestWrapper {
        String symbol;
        TickHistoryType type;
        Date begin;
        Date end;
        Integer limit;
        CursorType cursorType;

        public String getSymbol() {
            return symbol;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        public TickHistoryType getType() {
            return type;
        }

        public void setType(TickHistoryType type) {
            this.type = type;
        }

        public Date getBegin() {
            return begin;
        }

        public void setBegin(Date begin) {
            this.begin = begin;
        }

        public Date getEnd() {
            return end;
        }

        public void setEnd(Date end) {
            this.end = end;
        }

        public Integer getLimit() {
            return limit;
        }

        public void setLimit(Integer limit) {
            this.limit = limit;
        }

        public CursorType getCursorType() {
            return cursorType;
        }

        public void setCursorType(CursorType cursorType) {
            this.cursorType = cursorType;
        }
    }

    static class TickHistoryCallback extends ATCallback implements ATTickHistoryResponseCallback {
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
        public void process(long requestId, ATServerAPIDefines.ATTickHistoryResponseType responseType, TickHistoryDbResponseCollection collection) {
            logger.info("TickHistoryCallback: {}, {}", requestId, responseType.m_responseType);

            if (responseType.m_responseType == ATServerAPIDefines.ATTickHistoryResponseType.TickHistoryResponseSuccess) {
                for (ATServerAPIDefines.ATTICKHISTORY_RECORD item : collection.GetRecords()) {
                    Map<String, Object> kv = new HashMap<>();
                    switch (item.recordType.m_historyRecordType) {
                        case ATServerAPIDefines.ATTickHistoryRecordType.TickHistoryRecordTrade:
                            ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD trade = (ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD) item;
                            kv.put("type", TickHistoryType.T);
                            kv.put("price", trade.lastPrice.price);
                            kv.put("size", trade.lastSize);
                            kv.put("time", MarketDataUtils.systemTimeToTimestamp(trade.lastDateTime).getTimeInMillis());
                            kv.put("condition", Arrays.stream(trade.lastCondition).map(it -> it.m_atTradeConditionType));
                            kv.put("exchange", trade.lastExchange.m_atExchangeType);
                            break;

                        case ATServerAPIDefines.ATTickHistoryRecordType.TickHistoryRecordQuote:
                            ATServerAPIDefines.ATTICKHISTORY_QUOTE_RECORD quote = (ATServerAPIDefines.ATTICKHISTORY_QUOTE_RECORD) item;
                            kv.put("type", TickHistoryType.Q);
                            kv.put("time", MarketDataUtils.systemTimeToTimestamp(quote.quoteDateTime).getTimeInMillis());
                            kv.put("askPrice", quote.askPrice.price);
                            kv.put("askSize", quote.askSize);
                            kv.put("askExchange", quote.askExchange.m_atExchangeType);
                            kv.put("bidPrice", quote.bidPrice.price);
                            kv.put("bidSize", quote.bidSize);
                            kv.put("bidExchange", quote.bidExchange.m_atExchangeType);
                            kv.put("condition", quote.quoteCondition.m_quoteConditionType);
                            break;
                    }
                    list.add(kv);
                }
            }
            countDownLatch.countDown();
        }
    }
}
