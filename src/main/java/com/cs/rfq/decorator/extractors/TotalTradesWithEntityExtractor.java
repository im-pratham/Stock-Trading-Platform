package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TotalTradesWithEntityExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> negativeTrades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        BigInteger tradesToday = BigInteger.valueOf(filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(todayMs))).count());
        BigInteger tradesPastWeek = BigInteger.valueOf(filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs))).count());
        BigInteger tradesPastYear = BigInteger.valueOf(filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastYearMs))).count());

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradesWithEntityToday, tradesToday);
        results.put(tradesWithEntityPastWeek, tradesPastWeek);
        results.put(tradesWithEntityPastYear, tradesPastYear);
        return results;
    }

}
