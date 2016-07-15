package vt.cs.analyzer.mapred;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import vt.cs.analyzer.mapred.AnalysisMapper.ErrorCounter;
import vt.cs.smells.analyzer.AnalysisException;
import vt.cs.smells.analyzer.AnalysisManager;
import vt.cs.smells.analyzer.Main;
import vt.cs.smells.analyzer.parser.ParsingException;
import vt.cs.smells.analyzer.parser.Util;


public class TestAnalysisMapper {
	MapDriver<Object, Text, LongWritable, Text> mapDriver;
	private String[] lines;
	private AnalysisManager blockAnalyzer;
	private JSONParser jsonParser;

    @Before
    public void setup() throws IOException {
    	AnalysisMapper mapper = new AnalysisMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        blockAnalyzer = new AnalysisManager();
        jsonParser = new JSONParser();
    }
    
    @SuppressWarnings("unchecked")
	@Test
    public void testOutputIDandAnalysisResult() throws Exception {
//    	InputStream in = Main.class.getClassLoader().getResource("hdfs_input_files/sources-0.json").openStream();    	
//    	lines = IOUtils.toString(in).split("\n");
    	JSONObject line = new JSONObject();
    	line.put("_id", 104240489);
    	String src = Util.retrieveProjectOnline(104240489);
    	line.put("src", src);
    	Text value = new Text(line.toJSONString());

        
		JSONObject record = (JSONObject) jsonParser.parse(value.toString());
		LongWritable projectID = new LongWritable( (Long) record.get("_id"));
//		String src = record.get("src").toString();
//        JSONObject report = blockAnalyzer.analyze(src);
        blockAnalyzer.analyze(src);
        JSONObject report = blockAnalyzer.getConciseJSONReports();
        
        mapDriver.withInput(new LongWritable(), value)
                .withOutput(projectID, new Text(report.toJSONString())) 
                .runTest();
    } 
    
    @Test
    public void testNoOutputForMismatchedID() throws IOException, ParseException, ParsingException, AnalysisException{
    	InputStream in = Main.class.getClassLoader().getResource("hdfs_input_files/mismatchedID.json").openStream();
    	lines = IOUtils.toString(in).split("\n");    	
    	Text value = new Text(lines[0]);
        
        
		JSONObject record = (JSONObject) jsonParser.parse(value.toString());
		LongWritable projectID = new LongWritable( (Long) record.get("_id"));
		String src = record.get("src").toString();
		blockAnalyzer.analyze(src);
        JSONObject report = blockAnalyzer.getConciseJSONReports();
        
        mapDriver.withInput(new LongWritable(), value)
                .runTest();
        
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(ErrorCounter.MISMATCHED_PROJECT_ID).getValue());
    }
    
}
