package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ATCallback;
import at.feedapi.BarHistoryDbResponseCollection;
import at.feedapi.Helpers;
import at.shared.ATServerAPIDefines;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketDataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CreateBarHistoryCommand extends MarketAPIBaseCommand<List<Map<String, Object>>> {
    private static final Logger logger = LoggerFactory.getLogger(CreateBarHistoryCommand.class);

    public CreateBarHistoryCommand(MarketData marketData) {
        super(marketData);
    }

    @Override
    public List<Map<String, Object>> handle(CommandRequestWrapper request) {
        CreateBarHistoryRequest payload = (CreateBarHistoryRequest) request;
        ATServerAPIDefines.ATSYMBOL symbol = Helpers.StringToSymbol(payload.getSymbol());

        logger.info("handle: {}", payload);

        BarHistoryCallback callback = new BarHistoryCallback();
        long requestId;

        if (payload.getBegin() != null && payload.getEnd() != null) {
            requestId = marketData.serverAPI.ATCreateBarHistoryDbRequest(
                    marketData.session,
                    symbol,
                    (new ATServerAPIDefines()).new ATBarHistoryType(Byte.parseByte(payload.getType().value)),
                    payload.getFrequency().shortValue(),
                    dateTimeToSystemTime(payload.getBegin()),
                    dateTimeToSystemTime(payload.getEnd()),
                    callback
            );
        } else if (payload.getBegin() != null && payload.getLimit() != null) {
            requestId = marketData.serverAPI.ATCreateBarHistoryDbRequest(
                    marketData.session,
                    symbol,
                    (new ATServerAPIDefines()).new ATBarHistoryType(Byte.parseByte(payload.getType().value)),
                    payload.getFrequency().shortValue(),
                    dateTimeToSystemTime(payload.getBegin()),
                    payload.getLimit(),
                    (new ATServerAPIDefines()).new ATCursorType(Byte.parseByte(payload.getCursorType().value)),
                    callback
            );
        } else if (payload.getLimit() != null) {
            requestId = marketData.serverAPI.ATCreateBarHistoryDbRequest(
                    marketData.session,
                    symbol,
                    (new ATServerAPIDefines()).new ATBarHistoryType(Byte.parseByte(payload.getType().value)),
                    payload.getFrequency().shortValue(),
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

    public enum BarHistoryType {
        Intraday("0"),
        Daily("1"),
        Weekly("2");

        public final String value;

        private BarHistoryType(String value) {
            this.value = value;
        }
    }

    public enum CursorType {
        CursorBackward("1"),
        CursorForward("2");

        public final String value;

        private CursorType(String value) {
            this.value = value;
        }
    }

    public static class CreateBarHistoryRequest extends CommandRequestWrapper {
        String symbol;
        BarHistoryType type;
        Date begin;
        Date end;
        Integer limit;
        Integer frequency;
        CursorType cursorType;

        public CursorType getCursorType() {
            return cursorType;
        }

        public void setCursorType(CursorType cursorType) {
            this.cursorType = cursorType;
        }

        public String getSymbol() {
            return symbol;
        }

        public void setSymbol(String symbol) {
            this.symbol = symbol;
        }

        public BarHistoryType getType() {
            return type;
        }

        public void setType(BarHistoryType type) {
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

        public Integer getFrequency() {
            return frequency == null ? 0 : frequency;
        }

        public void setFrequency(Integer frequency) {
            this.frequency = frequency;
        }
    }

    static class BarHistoryCallback extends ATCallback implements ATBarHistoryResponseCallback {
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
        public void process(long requestId, ATServerAPIDefines.ATBarHistoryResponseType responseType, BarHistoryDbResponseCollection collection) {
            logger.info("BarHistoryCallback: {}, {}, {}", requestId, responseType.m_responseType, collection.GetRecords().size());

            if (responseType.m_responseType == ATServerAPIDefines.ATBarHistoryResponseType.BarHistoryResponseSuccess) {
                for (ATServerAPIDefines.ATBARHISTORY_RECORD item : collection.GetRecords()) {
                    Map<String, Object> kv = new HashMap<>();

                    kv.put("time", MarketDataUtils.systemTimeToTimestamp(item.barTime).getTimeInMillis());
                    kv.put("open", item.open.price);
                    kv.put("high", item.high.price);
                    kv.put("low", item.high.price);
                    kv.put("close", item.close.price);
                    kv.put("volume", item.volume);

//                    logger.info("BarHistoryCallback KV: {}", kv);


                    list.add(kv);
                }
            }

            countDownLatch.countDown();
        }
    }
}
