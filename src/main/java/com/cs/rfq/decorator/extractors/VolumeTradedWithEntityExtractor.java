package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class VolumeTradedWithEntityExtractor implements RfqMetadataExtractor {
    private String sqlQuery = "SELECT sum(LastQty) from trade where EntityId='%s' AND TraderId='%s' AND TradeDate >= '%s'";

    public VolumeTradedWithEntityExtractor() {
        this.todaysDate = DateTime.now();
    }

    private DateTime todaysDate;

    public void setTodaysDate(DateTime todaysDate) {
        this.todaysDate = todaysDate;
    }


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String todayDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.toDate());
        String pastWeekDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.minusWeeks(1).toDate());
        String pastMonthDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.minusMonths(1).toDate());
        String pastYearDate = new SimpleDateFormat("yyyy-MM-dd").format(this.todaysDate.minusYears(1).toDate());

        trades.createOrReplaceTempView("trade");

        Dataset<Row> sqlQueryResultsToday = session.sql(String.format(sqlQuery,
                rfq.getEntityId(),
                rfq.getTraderId(),
                todayDate));
        Dataset<Row> sqlQueryResultsPastWeek = session.sql(String.format(sqlQuery,
                rfq.getEntityId(),
                rfq.getTraderId(),
                pastWeekDate));
        Dataset<Row> sqlQueryResultsPastMonth = session.sql(String.format(sqlQuery,
                rfq.getEntityId(),
                rfq.getTraderId(),
                pastMonthDate));
        Dataset<Row> sqlQueryResultsPastYear = session.sql(String.format(sqlQuery,
                rfq.getEntityId(),
                rfq.getTraderId(),
                pastYearDate));

        // calculate volumes
        Object volumeToday = sqlQueryResultsToday.first().get(0);
        if (volumeToday == null) {
            volumeToday = 0L;
        }
        Object volumePastWeek = sqlQueryResultsPastWeek.first().get(0);
        if (volumePastWeek == null) {
            volumePastWeek = 0L;
        }
        Object volumePastMonth = sqlQueryResultsPastMonth.first().get(0);
        if (volumePastMonth == null) {
            volumePastMonth = 0L;
        }
        Object volumePastYear = sqlQueryResultsPastYear.first().get(0);
        if (volumePastYear == null) {
            volumePastYear = 0L;
        }

        // Put the results
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(RfqMetadataFieldNames.volumeTradedWithEntityToday, volumeToday);
        results.put(RfqMetadataFieldNames.volumeTradedWithEntityPastWeek, volumePastWeek);
        results.put(RfqMetadataFieldNames.volumeTradedWithEntityPastMonth, volumePastMonth);
        results.put(RfqMetadataFieldNames.volumeTradedWithEntityPastYear, volumePastYear);

        return results;
    }
}