package com.ygmpkk.marketdata.agent.web.rest;

import com.ygmpkk.marketdata.agent.service.marketapi.command.*;
import com.ygmpkk.marketdata.agent.service.marketapi.command.CreateMoversSubscriptionCommand.CreateMoversStreamRequest;
import com.ygmpkk.marketdata.agent.service.marketapi.command.CreateSubscriptionCommand.CreateSubscriptionStreamRequest;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

import static com.ygmpkk.marketdata.agent.service.marketapi.command.CreateBarHistoryCommand.*;
import static com.ygmpkk.marketdata.agent.service.marketapi.command.CreateConstituentCommand.*;
import static com.ygmpkk.marketdata.agent.service.marketapi.command.CreateMarketHolidayCommand.*;
import static com.ygmpkk.marketdata.agent.service.marketapi.command.CreateMarketMoversCommand.*;
import static com.ygmpkk.marketdata.agent.service.marketapi.command.CreateQuoteDbCommand.*;
import static com.ygmpkk.marketdata.agent.service.marketapi.command.CreateTickHistoryCommand.*;

@RestController
@RequestMapping("/v1")
public class MarketAPIV1Controller {
    final MarketData marketData;

    public MarketAPIV1Controller(MarketData marketData) {
        this.marketData = marketData;
    }

    @PostMapping("/last_quotes")
    public Object getQuotes(@RequestBody CreateQuoteRequest commandRequest) {
        CreateQuoteDbCommand command = new CreateQuoteDbCommand(marketData);
        return command.handle(commandRequest);
    }

    @PostMapping("/tick_histories")
    public Object getTickHistories(@RequestBody CreateTickHistoryRequest commandRequest) {
        CreateTickHistoryCommand command = new CreateTickHistoryCommand(marketData);
        return command.handle(commandRequest);
    }

    @PostMapping("/bar_histories")
    public Object getBarHistories(@RequestBody CreateBarHistoryRequest commandRequest) {
        CreateBarHistoryCommand command = new CreateBarHistoryCommand(marketData);
        return command.handle(commandRequest);
    }

    @PostMapping("/market_movers")
    public Object getMarketMovers(@RequestBody CreateMarketMoversDbRequest commandRequest) {
        CreateMarketMoversCommand command = new CreateMarketMoversCommand(marketData);
        return command.handle(commandRequest);
    }

    @PostMapping("/constituents")
    public Object getConstituents(@RequestBody CreateConstituentRequest commandRequest) {
        CreateConstituentCommand command = new CreateConstituentCommand(marketData);
        return command.handle(commandRequest);
    }

    @PostMapping("/market_holiday")
    public Object getMarketHoliday(@RequestBody CreateMarketHolidayRequest commandRequest) {
        CreateMarketHolidayCommand command = new CreateMarketHolidayCommand(marketData);
        return command.handle(commandRequest);
    }

    @PostMapping("/subscription_streams")
    public Object subscriptionStreams(@RequestBody CreateSubscriptionStreamRequest commandRequest) {
        CreateSubscriptionCommand command = new CreateSubscriptionCommand(marketData);
        return command.handle(commandRequest);
    }

    @PostMapping("/subscription_movers")
    public Object subscriptionMovers(@RequestBody CreateMoversStreamRequest commandRequest) {
        CreateMoversSubscriptionCommand command = new CreateMoversSubscriptionCommand(marketData);
        return command.handle(commandRequest);
    }
}
