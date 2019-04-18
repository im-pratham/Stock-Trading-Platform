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

public class TradeSideBiasExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
        rfq.setTraderId(5561302304887460215L);
    }

    @Test
    public void checkAverageTradePriceWhenTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-06-18")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultPastMonth = meta.get(RfqMetadataFieldNames.tradeSideBiasPastMonth);
        Object resultPastWeek = meta.get(RfqMetadataFieldNames.tradeSideBiasPastWeek);

        assertEquals(new BigDecimal("2.06"), resultPastMonth);
        assertEquals(new BigDecimal("4.90231E+7"), resultPastWeek);
    }

    @Test
    public void checkAverageTradePriceWhenNoTradesMatch() throws ParseException {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2019-06-17")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultPastMonth = meta.get(RfqMetadataFieldNames.tradeSideBiasPastMonth);
        Object resultPastWeek = meta.get(RfqMetadataFieldNames.tradeSideBiasPastWeek);

        assertEquals(new BigDecimal("-1"), resultPastMonth);
        assertEquals(new BigDecimal("-1"), resultPastWeek);
    }

    @Test
    public void checkAverageTradePriceWhenNoBuySideTradesMatch() throws ParseException {
        rfq.setTraderId(6089985145335003723L);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setTodaysDate(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-06-19")));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, null);

        Object resultPastMonth = meta.get(RfqMetadataFieldNames.tradeSideBiasPastMonth);
        Object resultPastWeek = meta.get(RfqMetadataFieldNames.tradeSideBiasPastWeek);

        assertEquals(new BigDecimal("0.00"), resultPastMonth);
        assertEquals(new BigDecimal("-1"), resultPastWeek);
    }
}
