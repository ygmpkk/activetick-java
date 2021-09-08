package com.ygmpkk.marketdata.agent.service.marketdata;

import at.shared.ATServerAPIDefines;

import java.util.Calendar;
import java.util.TimeZone;

public class MarketDataUtils {
    public static Calendar systemTimeToTimestamp(ATServerAPIDefines.SYSTEMTIME systemtime) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
        calendar.set(systemtime.year, systemtime.month - 1, systemtime.day, systemtime.hour, systemtime.minute, systemtime.second);
        calendar.set(Calendar.MILLISECOND, systemtime.milliseconds);
        return  calendar;
    }
}
