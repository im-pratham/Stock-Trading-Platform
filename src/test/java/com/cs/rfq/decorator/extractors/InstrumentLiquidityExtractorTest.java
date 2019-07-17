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

public class InstrumentLiquidityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
        rfq.setEntityId(5561279226039690843L);
    }

    @Test
    public void checkInstrumentLiquidityWhenTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-06-17")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultPastMonth = meta.get(RfqMetadataFieldNames.instrumentLiquidityPastWeek);

        assertEquals(1_350_000L, resultPastMonth);
    }

    @Test
    public void checkInstrumentLiquidityWhenNoTradesMatch() throws ParseException {
        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-06-17")));

        rfq.setIsin("AT0000A0N900");
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultPastMonth = meta.get(RfqMetadataFieldNames.instrumentLiquidityPastWeek);

        assertEquals(0L, resultPastMonth);
    }

}
