package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.mortbay.log.Log;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {
    private String sqlQuery = "SELECT sum(LastQty*LastPx) from trade where TraderId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side=%d";

    public TradeSideBiasExtractor() {
        this.todaysDate = DateTime.now();
    }

    private DateTime todaysDate;

    public void setTodaysDate(DateTime todaysDate) {
        this.todaysDate = todaysDate;
    }


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> negativeTrades) {
        String pastMonthDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.minusMonths(1).toDate());
        String pastWeekDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.minusWeeks(1).toDate());

        trades.createOrReplaceTempView("trade");

        Dataset<Row> sqlQueryResultsPastMonthForBuy = session.sql(String.format(sqlQuery,
                rfq.getTraderId(),
                rfq.getIsin(),
                pastMonthDate,
                1));

        Dataset<Row> sqlQueryResultsPastMonthForSell = session.sql(String.format(sqlQuery,
                rfq.getTraderId(),
                rfq.getIsin(),
                pastMonthDate,
                2));

        Dataset<Row> sqlQueryResultsPastWeekForBuy = session.sql(String.format(sqlQuery,
                rfq.getTraderId(),
                rfq.getIsin(),
                pastWeekDate,
                1));

        Dataset<Row> sqlQueryResultsPastWeekForSell = session.sql(String.format(sqlQuery,
                rfq.getTraderId(),
                rfq.getIsin(),
                pastWeekDate,
                2));

        // calculate volumes
        Object amountPastMonthForBuy = sqlQueryResultsPastMonthForBuy.first().get(0);
        if (amountPastMonthForBuy == null) {
            amountPastMonthForBuy = 0.0;
        }
        Object amountPastMonthForSell = sqlQueryResultsPastMonthForSell.first().get(0);
        if (amountPastMonthForSell == null) {
            amountPastMonthForSell = 0.0;
        }
        Object amountPastWeekForBuy = sqlQueryResultsPastWeekForBuy.first().get(0);
        if (amountPastWeekForBuy == null) {
            amountPastWeekForBuy = 0.0;
        }
        Object amountPastWeekForSell = sqlQueryResultsPastWeekForSell.first().get(0);
        if (amountPastWeekForSell == null) {
            amountPastWeekForSell = 0.0;
        }
        
        // Put the results
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        
//        Log.info(String.format("%s %s %s %s",  amountPastMonthForBuy, amountPastMonthForSell, amountPastWeekForBuy, amountPastWeekForSell));
        
        if ((double)amountPastMonthForBuy == 0.0 && (double)amountPastMonthForSell == 0.0) {
            results.put(RfqMetadataFieldNames.tradeSideBiasPastMonth, new BigDecimal("-1"));
        } else if ((double)amountPastMonthForSell == 0.0) {
            results.put(RfqMetadataFieldNames.tradeSideBiasPastMonth, new BigDecimal(amountPastMonthForBuy.toString()));
        } else {
            BigDecimal ratio = new BigDecimal(amountPastMonthForBuy.toString())
                    .divide(new BigDecimal(amountPastMonthForSell.toString()), 2, RoundingMode.HALF_UP);
            results.put(RfqMetadataFieldNames.tradeSideBiasPastMonth, ratio);
        }

        if ((double)amountPastWeekForBuy == 0.0 && (double)amountPastWeekForSell == 0.0) {
            results.put(RfqMetadataFieldNames.tradeSideBiasPastWeek, new BigDecimal("-1"));
        } else if ((double)amountPastWeekForSell == 0.0) {
            results.put(RfqMetadataFieldNames.tradeSideBiasPastWeek, new BigDecimal(amountPastWeekForBuy.toString()));
        } else {
            BigDecimal ratio = new BigDecimal(amountPastWeekForBuy.toString())
                    .divide(new BigDecimal(amountPastWeekForSell.toString()), 2, RoundingMode.HALF_UP);
            results.put(RfqMetadataFieldNames.tradeSideBiasPastWeek, ratio);
        }

        return results;
    }
}