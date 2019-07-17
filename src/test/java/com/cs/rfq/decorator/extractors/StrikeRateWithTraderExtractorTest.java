package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StrikeRateWithTraderExtractorTest  extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setTraderId(6089985145335003723L);
    }

    @Test
    public void checkStrikeRateWhenAllTradesMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> negativeTrades = new TradeDataLoader().loadTrades(session, filePath);

        StrikeRateWithTraderExtractor extractor = new StrikeRateWithTraderExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, negativeTrades);

        double result = (double) meta.get(RfqMetadataFieldNames.strikeRateWithTrader);

        assertEquals(50.0, result, 0.0000001);
    }

    @Test
    public void checkStrikeRateWhenNoNegativeTradesMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> negativeTrades = new TradeDataLoader().loadTrades(session, filePath);

        rfq.setTraderId(6915717929522265936L);

        //all test trade data are for 2018 so this will cause no matches
        StrikeRateWithTraderExtractor extractor = new StrikeRateWithTraderExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, negativeTrades);

        double result = (double) meta.get(RfqMetadataFieldNames.strikeRateWithTrader);

        assertEquals(100.0, result, 0.0);
    }

    @Test
    public void checkStrikeRateWhenNoPositiveTradesMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> negativeTrades = new TradeDataLoader().loadTrades(session, filePath);

        rfq.setTraderId(5419847817764717882L);

        //all test trade data are for 2018 so this will cause no matches
        StrikeRateWithTraderExtractor extractor = new StrikeRateWithTraderExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades, negativeTrades);

        double result = (double) meta.get(RfqMetadataFieldNames.strikeRateWithTrader);

        assertEquals(0.0, result, 0.0);
    }
}