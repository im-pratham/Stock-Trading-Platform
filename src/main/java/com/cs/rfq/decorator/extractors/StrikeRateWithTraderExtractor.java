package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.mortbay.log.Log;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class StrikeRateWithTraderExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> negativeTrades) {
        Dataset<Row> filtered = trades
                .filter(trades.col("TraderId").equalTo(rfq.getTraderId()));

        double totalPositiveTrades = filtered.count();

        filtered = negativeTrades
                .filter(negativeTrades.col("TraderId").equalTo(rfq.getTraderId()));

        double totalNegativeTrades = filtered.count();

//        Log.info("POS / Neg " + totalPositiveTrades + "/" + totalNegativeTrades);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        double strikeRate = 0;

        // check for divide by zero condition
        if (totalPositiveTrades > 0) {
            strikeRate = (totalPositiveTrades / (totalPositiveTrades + totalNegativeTrades)) * 100;
        }
        results.put(strikeRateWithTrader, strikeRate);
        return results;
    }

}
