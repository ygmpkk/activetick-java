package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ATCallback;
import at.feedapi.MarketHolidaysResponseCollection;
import at.shared.ATServerAPIDefines;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketDataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CreateMarketHolidayCommand extends MarketAPIBaseCommand<List<Map<String, Object>>> {
    private static final Logger logger = LoggerFactory.getLogger(CreateMarketHolidayCommand.class);

    public CreateMarketHolidayCommand(MarketData marketData) {
        super(marketData);
    }

    @Override
    public List<Map<String, Object>> handle(CommandRequestWrapper request) {
        CreateMarketHolidayRequest payload = (CreateMarketHolidayRequest) request;
        MarketHolidayCallback callback = new MarketHolidayCallback();

        logger.info("handle: {}, {}", payload.getYearsGoingBack(), payload.getYearsGoingForward());

        long requestId = marketData.serverAPI.ATCreateMarketHolidayRequest(
                marketData.session,
                payload.getYearsGoingForward().shortValue(),
                payload.getYearsGoingForward().shortValue(),
                callback
        );

        sendRequest(requestId);

        return callback.getData();
    }

    public static class CreateMarketHolidayRequest extends CommandRequestWrapper {
        Integer yearsGoingBack;
        Integer yearsGoingForward;

        public Integer getYearsGoingBack() {
            return yearsGoingBack;
        }

        public void setYearsGoingBack(Integer yearsGoingBack) {
            this.yearsGoingBack = yearsGoingBack;
        }

        public Integer getYearsGoingForward() {
            return yearsGoingForward;
        }

        public void setYearsGoingForward(Integer yearsGoingForward) {
            this.yearsGoingForward = yearsGoingForward;
        }
    }

    public static class MarketHolidayCallback extends ATCallback implements ATMarketHolidaysResponseCallback {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<Map<String, Object>> list = new ArrayList<>();

        List<Map<String, Object>> getData() {
            try {
                countDownLatch.await(5, TimeUnit.SECONDS);
                return list;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void process(long requestId, MarketHolidaysResponseCollection collection) {
            logger.info("process: {}, {}", requestId, collection.GetHolidays().size());

            for (ATServerAPIDefines.ATMARKET_HOLIDAYSLIST_ITEM item : collection.GetHolidays()) {
                Map<String, Object> kv = new HashMap<>();
                kv.put("beginDateTime", MarketDataUtils.systemTimeToTimestamp(item.beginDateTime).getTimeInMillis());
                kv.put("endDateTime", MarketDataUtils.systemTimeToTimestamp(item.endDateTime).getTimeInMillis());
                kv.put("symbolType", item.symbolType.m_atSymbolType);

                list.add(kv);
            }

            countDownLatch.countDown();
        }
    }
}
