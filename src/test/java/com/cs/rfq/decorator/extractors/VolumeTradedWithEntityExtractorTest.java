package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VolumeTradedWithEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setTraderId(1509345351319978288L);
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedWithEntityExtractor extractor = new VolumeTradedWithEntityExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-06-18")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultToday = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityToday);
        Object resultPastWeek = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastWeek);
        Object resultPastMonth = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastMonth);
        Object resultPastYear = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastYear);

        assertEquals(0L, resultToday);
        assertEquals(700_000L, resultPastWeek);
        assertEquals(1_100_000L, resultPastMonth);
        assertEquals(1_100_000L, resultPastYear);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedWithEntityExtractor extractor = new VolumeTradedWithEntityExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2019-06-17")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultToday = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityToday);
        Object resultPastWeek = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastWeek);
        Object resultPastMonth = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastMonth);
        Object resultPastYear = meta.get(RfqMetadataFieldNames.volumeTradedWithEntityPastYear);

        assertEquals(0L, resultToday);
        assertEquals(0L, resultPastWeek);
        assertEquals(0L, resultPastMonth);
        assertEquals(0L, resultPastYear);
    }

}
