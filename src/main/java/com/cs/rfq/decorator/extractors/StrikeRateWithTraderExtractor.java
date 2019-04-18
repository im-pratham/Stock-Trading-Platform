package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.mortbay.log.Log;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class StrikeRateWithTraderExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> negativeTrades) {
        Dataset<Row> filtered = trades
                .filter(trades.col("TraderId").equalTo(rfq.getTraderId()));

        BigDecimal totalPositiveTrades = new BigDecimal(filtered.count());

        filtered = negativeTrades
                .filter(negativeTrades.col("TraderId").equalTo(rfq.getTraderId()));

        BigDecimal totalNegativeTrades = new BigDecimal(filtered.count());

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        BigDecimal strikeRate = BigDecimal.ZERO;

        // check for divide by zero condition
        if (totalPositiveTrades.compareTo(BigDecimal.ZERO) > 0) {
            strikeRate = totalPositiveTrades.divide(totalPositiveTrades.add(totalNegativeTrades), 2, RoundingMode.HALF_UP).multiply(new BigDecimal(100));
        }
        results.put(strikeRateWithTrader, strikeRate);
        return results;
    }
}
