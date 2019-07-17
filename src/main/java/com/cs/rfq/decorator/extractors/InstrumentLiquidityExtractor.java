package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {
    private String sqlQuery = "SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'";

    public InstrumentLiquidityExtractor() {
        this.todaysDate = DateTime.now();
    }

    private DateTime todaysDate;

    public void setTodaysDate(DateTime todaysDate) {
        this.todaysDate = todaysDate;
    }


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> negativeTrades) {
        String pastMonthDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.minusMonths(1).toDate());

        trades.createOrReplaceTempView("trade");

        Dataset<Row> sqlQueryResultsPastMonth = session.sql(String.format(sqlQuery,
                rfq.getEntityId(),
                rfq.getIsin(),
                pastMonthDate));

        // calculate volumes
        Object volumePastMonth = sqlQueryResultsPastMonth.first().get(0);
        if (volumePastMonth == null) {
            volumePastMonth = 0L;
        }

        // Put the results
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(RfqMetadataFieldNames.instrumentLiquidityPastWeek, volumePastMonth);

        return results;
    }
}