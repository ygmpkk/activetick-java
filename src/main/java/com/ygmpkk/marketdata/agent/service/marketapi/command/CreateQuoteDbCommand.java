package com.ygmpkk.marketdata.agent.service.marketapi.command;

import at.feedapi.ATCallback;
import at.feedapi.Helpers;
import at.feedapi.QuoteDbResponseCollection;
import at.shared.ATServerAPIDefines;
import at.shared.ATServerAPIDefines.ATSYMBOL;
import at.shared.ATServerAPIDefines.ATQuoteFieldType;
import at.shared.ATServerAPIDefines.ATQuoteDbResponseType;
import at.shared.ATServerAPIDefines.QuoteDbResponseItem;
import at.shared.ActiveTick.DateTime;
import at.shared.ActiveTick.UInt64;
import com.ygmpkk.marketdata.agent.service.marketdata.MarketData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CreateQuoteDbCommand extends MarketAPIBaseCommand<List<Map<String, Object>>> {
    private static final Logger logger = LoggerFactory.getLogger(CreateQuoteDbCommand.class);

    public CreateQuoteDbCommand(MarketData marketData) {
        super(marketData);
    }

    public List<Map<String, Object>> handle(CommandRequestWrapper _payload) {
        CreateQuoteRequest payload = (CreateQuoteRequest) _payload;
//        logger.info("handle: {}", payload);

        List<ATSYMBOL> symbols = new ArrayList<>();


        Arrays.asList(payload.getSymbols()).forEach(item -> symbols.add(Helpers.StringToSymbol(item)));

        List<ATQuoteFieldType> fieldTypes = new ArrayList<>();
        ATServerAPIDefines atServerAPIDefines = new ATServerAPIDefines();
        Arrays.asList(payload.getFieldTypes()).forEach(item -> fieldTypes.add((atServerAPIDefines.new ATQuoteFieldType(item.shortValue()))));

//        logger.info("symbols: {}", new String(symbols.get(0).symbol));
//        logger.info("fieldTypes: {}", fieldTypes.get(0).m_atQuoteFieldType);
//        logger.info("session: {}", marketData.session);

        QuoteDbResponseCallback callback = new QuoteDbResponseCallback();
        long requestId = marketData.serverAPI.ATCreateQuoteDbRequest(
                marketData.session,
                symbols,
                fieldTypes,
                callback
        );

        sendRequest(requestId);

//        logger.info("requestId: {}", requestId);

        return callback.getData();
    }


    static class QuoteDbResponseCallback extends ATCallback implements ATQuoteDbResponseCallback {
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
        public void process(long requestId, ATQuoteDbResponseType responseType, QuoteDbResponseCollection collection) {
//            logger.info("process requestId: {}", requestId);
            if (responseType.m_atQuoteDbResponseType == ATServerAPIDefines.ATQuoteDbResponseType.QuoteDbResponseSuccess) {
                for (QuoteDbResponseItem responseItem : collection.GetItems()) {
                    Map<String, Object> kv = new HashMap<>();
                    Iterator<ATServerAPIDefines.QuoteDbDataItem> item = responseItem.m_vecDataItems.iterator();
                    String symbol = new String(responseItem.m_atResponse.symbol.symbol).replaceAll("\u0000", "");
                    if (responseItem.m_atResponse.symbol.symbolType == ATServerAPIDefines.ATSymbolType.StockOption) {
                        kv.put("symbol", "." + symbol);
                    } else {
                        kv.put("symbol", symbol);
                    }


                    while (responseItem.m_atResponse.status.m_atSymbolStatus == ATServerAPIDefines.ATSymbolStatus.SymbolStatusSuccess && item.hasNext()) {
                        ATServerAPIDefines.QuoteDbDataItem quoteDb = item.next();

                        byte[] intBytes = new byte[4];
                        byte[] longBytes = new byte[8];

//                        logger.info("fieldName: {} => {}, {}", symbol, quoteDb.m_dataItem.dataType.m_atDataType, FieldType.fromInt(
//                                quoteDb.m_dataItem.fieldType.m_atQuoteFieldType).name());

                        // 转换数据格式
                        switch (quoteDb.m_dataItem.dataType.m_atDataType) {
                            case ATServerAPIDefines.ATDataType.Byte:
                            case ATServerAPIDefines.ATDataType.String:
                            case ATServerAPIDefines.ATDataType.UnicodeString:
                                kv.put(FieldType.fromInt(
                                        quoteDb.m_dataItem.fieldType.m_atQuoteFieldType).name(),
                                        new String(quoteDb.GetItemData())
                                );
                                break;

                            case ATServerAPIDefines.ATDataType.ByteArray:
                                break;

                            case ATServerAPIDefines.ATDataType.DateTime:
                                UInt64 li = new UInt64(quoteDb.GetItemData());
                                ATServerAPIDefines.SYSTEMTIME dateTime = DateTime.GetDateTime(li);
                                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
                                calendar.set(dateTime.year, dateTime.month - 1, dateTime.day, dateTime.hour, dateTime.minute, dateTime.second);
                                calendar.set(Calendar.MILLISECOND, dateTime.milliseconds);

                                kv.put(FieldType.fromInt(
                                        quoteDb.m_dataItem.fieldType.m_atQuoteFieldType).name(),
                                        calendar
                                );
                                break;
                            case ATServerAPIDefines.ATDataType.Double:
                                kv.put(
                                        FieldType.fromInt(quoteDb.m_dataItem.fieldType.m_atQuoteFieldType).name(),
                                        bytes2Double(quoteDb.GetItemData())
                                );
                                break;

                            case ATServerAPIDefines.ATDataType.Integer32:
                            case ATServerAPIDefines.ATDataType.UInteger32:
                                System.arraycopy(quoteDb.GetItemData(), 0, intBytes, 0, 4);
                                kv.put(
                                        FieldType.fromInt(quoteDb.m_dataItem.fieldType.m_atQuoteFieldType).name(),
                                        ByteBuffer.wrap(intBytes).order(ByteOrder.LITTLE_ENDIAN).getInt()
                                );
                                break;

                            case ATServerAPIDefines.ATDataType.Integer64:
                            case ATServerAPIDefines.ATDataType.UInteger64:
                                System.arraycopy(quoteDb.GetItemData(), 0, longBytes, 0, 8);
                                kv.put(
                                        FieldType.fromInt(quoteDb.m_dataItem.fieldType.m_atQuoteFieldType).name(),
                                        ByteBuffer.wrap(longBytes).order(ByteOrder.LITTLE_ENDIAN).getLong()
                                );
                                break;

                            case ATServerAPIDefines.ATDataType.Price:
                                kv.put(
                                        FieldType.fromInt(quoteDb.m_dataItem.fieldType.m_atQuoteFieldType).name(),
                                        Helpers.BytesToPrice(quoteDb.GetItemData()).price
                                );
                                break;
                        }
                    }
                    list.add(kv);
                }
            }

            countDownLatch.countDown();
        }
    }

    public enum FieldType {
        symbol(1),
        open(2),
        preClose(3),
        close(4),
        last(5),
        bid(6),
        ask(7),
        high(8),
        low(9),
        dayHigh(10),
        dayLow(11),
        preMarketOpen(12),
        extendedHoursLast(13),
        afterMarketClose(14),
        bidExchange(15),
        askExchange(16),
        lastExchange(17),
        lastCondition(18),
        quoteCondition(19),
        lastTradeDateTime(20),
        lastQuoteDateTime(21),
        dayHighDateTime(22),
        dayLowDateTime(23),
        lastSize(24),
        bidSize(25),
        askSize(26),
        volume(27),
        preMarketVolume(28),
        afterMarketVolume(29),
        tradeCount(30),
        preMarketTradeCount(31),
        afterMarketTradeCount(32),
        profileShortName(33),
        profilePrimaryExchange(34),
        profileLongName(35),
        profileSector(36),
        profileIndustry(37),
        optionOpenInterest(100),
        optionStrikePrice(101),
        financialIncomeStatementBasicEPSTotalQtr(200),
        financialIncomeStatementBasicEPSTotalYear(201),
        financialIncomeStatementDilutedEPSTotalQtr(202),
        financialIncomeStatementDilutedEPSTotalYear(203),
        financialIncomeStatementDividendsPaidPerShareQtr(204),
        financialIncomeStatementDividendsPaidPerShareYear(205),
        financialIncomeStatementDateQtr(206),
        financialIncomeStatementDateYear(207),
        financialBalanceSheetsTotalCommonSharesOutstandingQtr(600),
        financialBalanceSheetsTotalCommonSharesOutstandingYear(601),
        financialBalanceSheetsDateQtr(602),
        financialBalanceSheetsDateYear(603);

        public final int value;

        private FieldType(int value) {
            this.value = value;
        }

        private static final Map<Integer, FieldType> map;

        static {
            map = Arrays.stream(values()).collect(Collectors.toMap(e -> e.value, e -> e));
        }

        public static FieldType fromInt(short value) {
            return map.get(Short.valueOf(value).intValue());
        }

    }

    public static double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }

    public static class CreateQuoteRequest extends CommandRequestWrapper {
        String type;
        String[] symbols;
        Integer[] fieldTypes;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String[] getSymbols() {
            return symbols;
        }

        public void setSymbols(String[] symbols) {
            this.symbols = symbols;
        }

        public Integer[] getFieldTypes() {
            return fieldTypes;
        }

        public void setFieldTypes(Integer[] fieldTypes) {
            this.fieldTypes = fieldTypes;
        }
    }
}
