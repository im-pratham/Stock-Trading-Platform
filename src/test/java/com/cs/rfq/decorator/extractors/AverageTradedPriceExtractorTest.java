package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AverageTradedPriceExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
        rfq.setEntityId(5561279226039690843L);
    }

    @Test
    public void checkAverageTradePriceWhenTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        AverageTradedPriceExtractor extractor = new AverageTradedPriceExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-06-17")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultPastMonth = meta.get(RfqMetadataFieldNames.averageTradedPricePastWeek);

        assertEquals(new BigDecimal("125.04"), resultPastMonth);
    }

    @Test
    public void checkAverageTradePriceWhenNoTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        AverageTradedPriceExtractor extractor = new AverageTradedPriceExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2019-06-17")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultPastMonth = meta.get(RfqMetadataFieldNames.averageTradedPricePastWeek);

        assertEquals(new BigDecimal("0"), resultPastMonth);
    }

}
