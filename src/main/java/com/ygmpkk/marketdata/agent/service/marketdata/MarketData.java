package com.ygmpkk.marketdata.agent.service.marketdata;

import at.feedapi.*;
import at.shared.ATServerAPIDefines;
import at.utils.jlib.OutputMessage;
import com.google.gson.Gson;
import com.ygmpkk.marketdata.agent.config.MarketdataConfiguration;
import com.ygmpkk.marketdata.agent.service.marketapi.MarketAPIPubSub;
import com.ygmpkk.marketdata.agent.service.marketdata.event.MarketDataEvent;
import com.ygmpkk.marketdata.agent.service.marketdata.event.MarketDataEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Calendar;

import static at.shared.ATServerAPIDefines.ATSessionStatusType.*;

@Service
public class MarketData implements ApplicationListener<MarketDataEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MarketData.class);
    private static final ATServerAPIDefines apiDefines = new ATServerAPIDefines();

    private final MarketDataPublisher publisher;
    private final MarketdataConfiguration marketdataConfiguration;
    private final MarketAPIPubSub marketAPIPubSub;

    public ActiveTickServerAPI serverAPI = new ActiveTickServerAPI();
    public Session session;

    public Requestor requestor;
    public Streamer streamer;

    public MarketData(MarketDataPublisher publisher, MarketdataConfiguration marketdataConfiguration, MarketAPIPubSub marketAPIPubSub) {
        this.publisher = publisher;
        this.marketdataConfiguration = marketdataConfiguration;
        this.marketAPIPubSub = marketAPIPubSub;
    }

    @PostConstruct
    private void init() {
        logger.info("ActiveTick init: {}", serverAPI.GetAPIVersionInformation());

        serverAPI = new ActiveTickServerAPI();
        serverAPI.ATInitAPI();
        session = serverAPI.ATCreateSession();
    }

    @Override
    public void onApplicationEvent(MarketDataEvent event) {
        logger.info("onApplicationEvent: {}", event.getEventType());

        switch (event.getEventType()) {
            case CONNECT:
                this.startSession();
                break;

            case LOGIN:
                this.login();
                break;

            case SUBSCRIBE:
                this.subscribe();
                break;

            default:
                logger.warn("ActiveEvent not found: {}", event.getEventType());
                break;
        }
    }

    private void startSession() {
        logger.info("connecting to {}", marketdataConfiguration.getPrimaryServer());

        session = serverAPI.ATCreateSession();
        streamer = new Streamer(session);
        requestor = new Requestor(serverAPI, session, streamer);

        session.SetServerTimeUpdateCallback(new ServerTimeUpdateCallback());
        session.SetOutputMessageCallback(new OutputMessageCallback());
//        session.SetStreamUpdateCallback(new StreamUpdateCallback());

        ATServerAPIDefines.ATGUID atguid = apiDefines.new ATGUID();
        atguid.SetGuid(marketdataConfiguration.getApikey());
        serverAPI.ATSetAPIKey(session, atguid);
        serverAPI.ATInitSession(
                session,
                marketdataConfiguration.getPrimaryServer(),
                marketdataConfiguration.getSecondServer(),
                marketdataConfiguration.getPort(),
                new SessionStatusCallback()
        );
    }

    private void login() {
        logger.info("login for: {}", marketdataConfiguration.getUserid());

        long requestId = serverAPI.ATCreateLoginRequest(
                session,
                marketdataConfiguration.getUserid(),
                marketdataConfiguration.getPassword(),
                new LoginCallback()
        );

        serverAPI.ATSendRequest(session, requestId, ActiveTickServerAPI.DEFAULT_REQUEST_TIMEOUT, new RequestTimeoutCallback());
        logger.info("login for requestId: {}", requestId);
    }

    private void subscribe() {
        logger.warn("subscribe ready");
    }

    private void disconnect() {
        serverAPI.ATShutdownSession(session);
        serverAPI.ATShutdownAPI();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////// callbacks

    private class SessionStatusCallback extends ATCallback implements ATCallback.ATSessionStatusChangeCallback {
        @Override
        public void process(Session session, ATServerAPIDefines.ATSessionStatusType atSessionStatusType) {
            Map<String, Object> map = new HashMap<>();
            switch (atSessionStatusType.m_atSessionStatusType) {
                case SessionStatusConnected:
                    logger.info("SessionStatusConnected: {}", session.IsConnected());
                    map.put("session", "SessionStatusConnected");
                    publisher.pushEvent(MarketDataEventType.LOGIN);
                    break;

                case SessionStatusDisconnected:
                    logger.info("SessionStatusDisconnected");
                    map.put("session", "SessionStatusDisconnected");
                    break;

                case SessionStatusDisconnectedDuplicateLogin:
                    logger.info("SessionStatusDisconnectedDuplicateLogin");
                    map.put("session", "SessionStatusDisconnectedDuplicateLogin");
                    break;

                case SessionStatusDisconnectedInactivity:
                    logger.info("SessionStatusDisconnectedInactivity");
                    map.put("session", "SessionStatusDisconnectedInactivity");
                    break;

                default:
                    logger.info("Unknown sessionStatusType");
                    map.put("session", "SessionUnknown");
                    break;
            }
            marketAPIPubSub.pub(new Gson().toJson(map));
        }
    }

    private class LoginCallback extends ATCallback implements ATCallback.ATLoginResponseCallback {
        @Override
        public void process(Session session, long requestId, ATServerAPIDefines.ATLOGIN_RESPONSE response) {
            Map<String, Object> map = new HashMap<>();

            switch (response.loginResponse.m_atLoginResponseType) {
                case ATServerAPIDefines.ATLoginResponseType.LoginResponseSuccess:
                    logger.info("LoginResponseSuccess");
                    map.put("login", "LoginResponseSuccess");

                    publisher.pushEvent(MarketDataEventType.SUBSCRIBE);
                    break;
                case ATServerAPIDefines.ATLoginResponseType.LoginResponseInvalidPassword:
                    logger.info("LoginResponseInvalidPassword");
                    map.put("login", "LoginResponseInvalidPassword");
                    break;
                case ATServerAPIDefines.ATLoginResponseType.LoginResponseInvalidRequest:
                    logger.info("LoginResponseInvalidRequest");
                    map.put("login", "LoginResponseInvalidRequest");
                    break;
                case ATServerAPIDefines.ATLoginResponseType.LoginResponseInvalidUserid:
                    logger.info("LoginResponseInvalidUserid");
                    map.put("login", "LoginResponseInvalidUserid");
                    break;
                case ATServerAPIDefines.ATLoginResponseType.LoginResponseLoginDenied:
                    logger.info("LoginResponseLoginDenied");
                    map.put("login", "LoginResponseLoginDenied");
                    break;
                case ATServerAPIDefines.ATLoginResponseType.LoginResponseServerError:
                    logger.info("LoginResponseServerError");
                    map.put("login", "LoginResponseServerError");
                    break;
                default:
                    logger.info("Unknown");
                    map.put("login", "LoginUnknown");
                    break;
            }

            marketAPIPubSub.pub(new Gson().toJson(map));
        }
    }

    public static class ServerTimeUpdateCallback extends ATCallback implements ATCallback.ATServerTimeUpdateCallback {
        @Override
        public void process(ATServerAPIDefines.SYSTEMTIME systemtime) {
        }
    }

    public static class OutputMessageCallback extends ATCallback implements ATCallback.ATOutputMessageCallback {
        @Override
        public void process(OutputMessage outputMessage) {
            logger.warn("OutputMessage => {}: {}", outputMessage.GetSeverity().C, outputMessage.GetMessage());
        }
    }

    public static class RequestTimeoutCallback extends ATCallback implements ATCallback.ATRequestTimeoutCallback {
        @Override
        public void process(long requestId) {
            logger.info("RequestTimeout: {}", requestId);
        }
    }

    public static class Requestor extends ActiveTickServerRequester {
        public Requestor(ActiveTickServerAPI activeTickServerAPI, Session session, ActiveTickStreamListener activeTickStreamListener) {
            super(activeTickServerAPI, session, activeTickStreamListener);
        }

        public void OnRequestTimeoutCallback(long requestId) {
            logger.info("Requestor request timeout: {}", requestId);
        }

        public void OnQuoteStreamResponse(long requestId, ATServerAPIDefines.ATStreamResponseType responseType, Vector<ATServerAPIDefines.ATQUOTESTREAM_DATA_ITEM> vector) {
            String strResponseType = "";
            switch (responseType.m_responseType) {
                case ATServerAPIDefines.ATStreamResponseType.StreamResponseSuccess:
                    strResponseType = "StreamResponseSuccess";
                    break;
                case ATServerAPIDefines.ATStreamResponseType.StreamResponseInvalidRequest:
                    strResponseType = "StreamResponseInvalidRequest";
                    break;
                case ATServerAPIDefines.ATStreamResponseType.StreamResponseDenied:
                    strResponseType = "StreamResponseDenied";
                    break;
                default:
                    break;
            }

            logger.info("RECV ({}): Quote stream response [{}]", requestId, strResponseType);

            if (responseType.m_responseType == ATServerAPIDefines.ATStreamResponseType.StreamResponseSuccess) {
                String strSymbolStatus = "";
                for (ATServerAPIDefines.ATQUOTESTREAM_DATA_ITEM atDataItem : vector) {
                    switch (atDataItem.symbolStatus.m_atSymbolStatus) {
                        case ATServerAPIDefines.ATSymbolStatus.SymbolStatusSuccess:
                            strSymbolStatus = "SymbolStatusSuccess";
                            break;
                        case ATServerAPIDefines.ATSymbolStatus.SymbolStatusInvalid:
                            strSymbolStatus = "SymbolStatusInvalid";
                            break;
                        case ATServerAPIDefines.ATSymbolStatus.SymbolStatusUnavailable:
                            strSymbolStatus = "SymbolStatusUnavailable";
                            break;
                        case ATServerAPIDefines.ATSymbolStatus.SymbolStatusNoPermission:
                            strSymbolStatus = "SymbolStatusNoPermission";
                            break;
                        default:
                            break;
                    }

                    logger.warn("symbol: ({}) status [{}]", new String(atDataItem.symbol.symbol), strSymbolStatus);
                }
            }
        }

        public void process(long l, MarketHolidaysResponseCollection marketHolidaysResponseCollection) {
            logger.info("OnMarketHolidaysResponse PROCESS: {}", l);
        }

        public void OnMarketHolidaysResponse(long l, Vector<ATServerAPIDefines.ATMARKET_HOLIDAYSLIST_ITEM> vector) {
            logger.info("OnMarketHolidaysResponse: {}", l);
        }
    }

    public class Streamer extends ActiveTickStreamListener {
        Session session;

        public Streamer(Session session) {
            super(session, true);
            this.session = session;
        }

        public void OnATStreamQuoteUpdate(ATServerAPIDefines.ATQUOTESTREAM_QUOTE_UPDATE update) {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
            calendar.set(update.quoteDateTime.year, update.quoteDateTime.month - 1, update.quoteDateTime.day, update.quoteDateTime.hour, update.quoteDateTime.minute, update.quoteDateTime.second);
            calendar.set(Calendar.MILLISECOND, update.quoteDateTime.milliseconds);

            String symbol = new String(update.symbol.symbol, StandardCharsets.UTF_8).replaceAll("\u0000", "");
            String time = String.format("%02d:%02d:%02d.%03d", update.quoteDateTime.hour, update.quoteDateTime.minute, update.quoteDateTime.second, update.quoteDateTime.milliseconds);
            byte condition = update.condition.m_quoteConditionType;

            byte symbolType = update.symbol.symbolType;
            byte askExchange = update.askExchange.m_atExchangeType;
            double askPrice = update.askPrice.price;
            long askSize = update.askSize;

            byte bidExchange = update.bidExchange.m_atExchangeType;
            double bidPrice = update.bidPrice.price;
            long bidSize = update.bidSize;

            double mid = (update.askPrice.price + update.bidPrice.price) / 2;

            logger.info("Quote {}/{} time:{}, ask:{}/{}, bid:{}/{}, mid:{}",
                    symbol,
                    symbolType,
                    time,
                    askPrice,
                    askSize,
                    bidPrice,
                    bidSize,
                    mid
            );

            Map<String, Object> map = new HashMap<>();
            map.put("event", update.updateType.m_nUpdateType);
            map.put("type", symbolType);
            map.put("symbol", symbol);
            map.put("time", calendar.getTimeInMillis());
            map.put("askPrice", askPrice);
            map.put("askSize", askSize);
            map.put("askExchange", askExchange);
            map.put("bidPrice", bidPrice);
            map.put("bidSize", bidSize);
            map.put("bidExchange", bidExchange);
            map.put("condition", condition);

            marketAPIPubSub.pub(new Gson().toJson(map));
        }

        public void OnATStreamTradeUpdate(ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE update) {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
            calendar.set(update.lastDateTime.year, update.lastDateTime.month - 1, update.lastDateTime.day, update.lastDateTime.hour, update.lastDateTime.minute, update.lastDateTime.second);
            calendar.set(calendar.MILLISECOND, update.lastDateTime.milliseconds);

            int[] conditions = new int[4];
            for (int i = 0; i < update.condition.length; i++) {
                conditions[i] = update.condition[i].m_atTradeConditionType;
            }

            String symbol = new String(update.symbol.symbol, StandardCharsets.UTF_8).replaceAll("\u0000", "");
            int symbolType = update.symbol.symbolType;
            String time = String.format("%02d:%02d:%02d.%03d", update.lastDateTime.hour, update.lastDateTime.minute, update.lastDateTime.second, update.lastDateTime.milliseconds);
            double price = update.lastPrice.price;
            long size = update.lastSize;
            byte exchange = update.lastExchange.m_atExchangeType;
            int flags = update.flags.m_attradeMessageFlags;

            logger.info("Trade {}/{} time:{}, last:{}/{}",
                    symbol,
                    symbolType,
                    time,
                    price,
                    size
            );

            Map<String, Object> map = new HashMap<>();
            map.put("event", update.updateType.m_nUpdateType);
            map.put("type", symbolType);
            map.put("symbol", symbol);
            map.put("time", calendar.getTimeInMillis());
            map.put("price", price);
            map.put("size", size);
            map.put("exchange", exchange);
            map.put("conditions", conditions);
            map.put("flags", flags);

            marketAPIPubSub.pub(new Gson().toJson(map));
        }

        public void OnATStreamRefreshUpdate(ATServerAPIDefines.ATQUOTESTREAM_REFRESH_UPDATE update) {
            logger.info("OnATStreamRefreshUpdate => {}, Price: {}, Size: {}, Close: {}; High: {}", new String(update.symbol.symbol), update.lastPrice.price, update.lastSize, update.closePrice.price, update.highPrice.price);

            Map<String, Object> kv = new HashMap<>();
            kv.put("symbol", new String(update.symbol.symbol).replaceAll("\u0000", ""));
            kv.put("event", update.updateType.m_nUpdateType);
            kv.put("lastCondition", Arrays.stream(update.lastCondition).map(item -> item.m_atTradeConditionType));
            kv.put("quoteCondition", update.quoteCondition.m_quoteConditionType);
            kv.put("openPrice", update.openPrice.price);
            kv.put("lastPrice", update.lastPrice.price);
            kv.put("highPrice", update.highPrice.price);
            kv.put("lowPrice", update.lowPrice.price);
            kv.put("closePrice", update.closePrice.price);
            kv.put("prevClosePrice", update.prevClosePrice.price);
            kv.put("afterMarketClosePrice", update.afterMarketClosePrice.price);
            kv.put("bidPrice", update.bidPrice);
            kv.put("askPrice", update.askPrice);
            kv.put("lastExchange", update.lastExchange.m_atExchangeType);
            kv.put("bidExchange", update.bidExchange.m_atExchangeType);
            kv.put("askExchange", update.askExchange.m_atExchangeType);
            kv.put("bidSize", update.bidSize);
            kv.put("askSize", update.askSize);
            kv.put("lastSize", update.lastSize);
            kv.put("volume", update.volume);

            marketAPIPubSub.pub(new Gson().toJson(kv));
        }

        public void OnATStreamTopMarketMoversUpdate(ATServerAPIDefines.ATMARKET_MOVERS_STREAM_UPDATE update) {
            logger.info("OnATStreamTopMarketMoversUpdate => {}: {}", new String(update.marketMovers.symbol.symbol), update.marketMovers.status.m_atSymbolStatus);

            Map<String, Object> kv = new HashMap<>();
            ATServerAPIDefines.ATMARKET_MOVERS_RECORD record = update.marketMovers;

            kv.put("symbol", new String(record.symbol.symbol).replaceAll("\u0000", ""));
            kv.put("status", record.status.m_atSymbolStatus);
            kv.put("event", update.updateType.m_nUpdateType);
            kv.put("time", MarketDataUtils.systemTimeToTimestamp(update.lastUpdateTime).getTimeInMillis());

            List<Map<String, Object>> items = new ArrayList<>();
            for (ATServerAPIDefines.ATMARKET_MOVERS_ITEM item : record.items) {
                Map<String, Object> itemValues = new HashMap<>();

                itemValues.put("symbol", new String(item.symbol.symbol).replaceAll("\u0000", ""));
                itemValues.put("volume", item.volume);
                itemValues.put("time", MarketDataUtils.systemTimeToTimestamp(item.lastDateTime).getTimeInMillis());
                itemValues.put("preClose", item.closePrice.price);
                itemValues.put("price", item.lastPrice.price);
                items.add(itemValues);
            }

            kv.put("items", items);

            marketAPIPubSub.pub(new Gson().toJson(kv));
        }
    }
}
