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

public class VolumeTradedForInstrumentExtractorTest  extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedForInstrumentExtractor extractor = new VolumeTradedForInstrumentExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-06-17")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultToday = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentToday);
        Object resultPastWeek = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastWeek);
        Object resultPastMonth = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth);
        Object resultPastYear = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastYear);

        assertEquals(0L, resultToday);
        assertEquals(920_000L, resultPastWeek);
        assertEquals(1_520_000L, resultPastMonth);
        assertEquals(1_520_000L, resultPastYear);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedForInstrumentExtractor extractor = new VolumeTradedForInstrumentExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2019-06-17")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultToday = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentToday);
        Object resultPastWeek = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastWeek);
        Object resultPastMonth = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth);
        Object resultPastYear = meta.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastYear);

        assertEquals(0L, resultToday);
        assertEquals(0L, resultPastWeek);
        assertEquals(0L, resultPastMonth);
        assertEquals(0L, resultPastYear);
    }

}
