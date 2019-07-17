package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.mortbay.log.Log;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class AverageTradedPriceExtractor implements RfqMetadataExtractor {
    private String sqlQueryTotalPrice = "SELECT sum(LastQty * LastPx) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'";
    private String sqlQueryTotalQuantity = "SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'";

    public AverageTradedPriceExtractor() {
        this.todaysDate = DateTime.now();
    }

    private DateTime todaysDate;

    public void setTodaysDate(DateTime todaysDate) {
        this.todaysDate = todaysDate;
    }


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> negativeTrades) {
        String pastWeekDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.minusWeeks(1).toDate());

        trades.createOrReplaceTempView("trade");

        Dataset<Row> sqlQueryResultsPastWeek = session.sql(String.format(sqlQueryTotalPrice,
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeekDate));

        // calculate volumes
        Object totalPastWeek = sqlQueryResultsPastWeek.first().get(0);
        if (totalPastWeek == null) {
            totalPastWeek = 0L;
        }

        sqlQueryResultsPastWeek = session.sql(String.format(sqlQueryTotalQuantity,
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeekDate));

        // calculate volumes
        Object volumePastWeek = sqlQueryResultsPastWeek.first().get(0);
        if (volumePastWeek == null) {
            volumePastWeek = 0L;
        }

        BigDecimal averageTrade = BigDecimal.ZERO;
        // check for divide by zero condition
        if (!new BigDecimal(volumePastWeek.toString()).equals(BigDecimal.ZERO)) {
            averageTrade = new BigDecimal(totalPastWeek.toString()).divide(new BigDecimal(volumePastWeek.toString()), 2, RoundingMode.HALF_UP);
        }

        // Put the results
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(RfqMetadataFieldNames.averageTradedPricePastWeek, averageTrade);

        return results;
    }
}