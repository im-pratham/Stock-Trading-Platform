package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class VolumeTradedForInstrumentExtractor implements RfqMetadataExtractor {
    private String sqlQuery = "SELECT sum(LastQty) from trade where SecurityId='%s' AND TradeDate >= '%s'";

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String todayDate = new SimpleDateFormat("yyyy-MM-DD").format(DateTime.now());
        String pastWeekDate = new SimpleDateFormat("yyyy-MM-DD").format(DateTime.now().minusWeeks(1));
        String pastMonthDate = new SimpleDateFormat("yyyy-MM-DD").format(DateTime.now().minusMonths(1));
        String pastYearDate = new SimpleDateFormat("yyyy-MM-DD").format(DateTime.now().minusYears(1));

        trades.createOrReplaceTempView("trade");

        Dataset<Row> sqlQueryResultsToday = session.sql(String.format(sqlQuery,
                rfq.getIsin(),
                todayDate));
        Dataset<Row> sqlQueryResultsPastWeek = session.sql(String.format(sqlQuery,
                rfq.getIsin(),
                pastWeekDate));
        Dataset<Row> sqlQueryResultsPastMonth = session.sql(String.format(sqlQuery,
                rfq.getIsin(),
                pastMonthDate));
        Dataset<Row> sqlQueryResultsPastYear = session.sql(String.format(sqlQuery,
                rfq.getIsin(),
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

        results.put(RfqMetadataFieldNames.volumeTradedToday, volumeToday);
        results.put(RfqMetadataFieldNames.volumeTradedPastWeek, volumePastWeek);
        results.put(RfqMetadataFieldNames.volumeTradedPastMonth, volumePastMonth);
        results.put(RfqMetadataFieldNames.volumeTradedPastYear, volumePastYear);

        return results;
    }
}