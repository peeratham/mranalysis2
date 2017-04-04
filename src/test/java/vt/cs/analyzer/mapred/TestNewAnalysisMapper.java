package vt.cs.analyzer.mapred;

import java.io.IOException;
import java.io.StringReader;

import main.AnalysisManager;
import main.ScratchParser;
import main.Util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.junit.Test;

import AST.Program;



public class TestNewAnalysisMapper {
	MapDriver<Object, Text, LongWritable, Text> mapDriver;
	private String[] lines;
	private AnalysisManager analysisManager;
	private JSONParser jsonParser;

    @Before
    public void setup() throws IOException {
    	NewAnalysisMapper mapper = new NewAnalysisMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        jsonParser = new JSONParser();
    }
    
    @SuppressWarnings("unchecked")
	@Test
    public void testOutputIDandAnalysisResult() throws Exception {
    	JSONObject line = new JSONObject();
    	line.put("_id", 104240489);
    	StringReader srcReader = Util.retrieveProjectOnline(104240489);
    	String src = IOUtils.toString(srcReader);
    	JSONObject srcJson = (JSONObject) jsonParser.parse(src);
    	line.put("src", srcJson);
    	Text value = new Text(line.toJSONString());
        
		JSONObject record = (JSONObject) jsonParser.parse(value.toString());
		
		LongWritable projectID = new LongWritable( (Long) record.get("_id"));
		ScratchParser parser = new ScratchParser();
		
		srcReader = new StringReader(src);
		Program program = parser.parse(srcReader);
        String report = AnalysisManager.processWithTimer(program, false, 60);
//        System.out.println(report);
        mapDriver.withInput(new LongWritable(), value)
                .withOutput(projectID, new Text(report)) 
                .runTest();
    } 
    
//    @Test
//    public void testNoOutputForMismatchedID() throws IOException, ParseException, ParsingException, AnalysisException{
//    	InputStream in = Main.class.getClassLoader().getResource("hdfs_input_files/mismatchedID.json").openStream();
//    	lines = IOUtils.toString(in).split("\n");    	
//    	Text value = new Text(lines[0]);
//        
//        
//		JSONObject record = (JSONObject) jsonParser.parse(value.toString());
//		LongWritable projectID = new LongWritable( (Long) record.get("_id"));
//		String src = record.get("src").toString();
//		blockAnalyzer.analyze(src);
//        JSONObject report = blockAnalyzer.getConciseJSONReports();
//        System.out.println(report.toJSONString());
//        mapDriver.withInput(new LongWritable(), value)
//                .runTest();
//        
//        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
//                .findCounter(ErrorCounter.MISMATCHED_PROJECT_ID).getValue());
//    }
//    
//    @Test
//    public void testUndefinedBlockCount() throws ParseException, ParsingException, AnalysisException, IOException{
//    	int id = 10537166;
//		String src = Util.retrieveProjectOnline(id);
//    	JSONObject line = new JSONObject();
//    	line.put("_id", id);
//    	line.put("src", src);
//    	Text value = new Text(line.toJSONString());
//
//        mapDriver.withInput(new LongWritable(), value) 
//                .runTest();
//        System.out.println(mapDriver.getExpectedOutputs());
//        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
//                .findCounter(ErrorCounter.UNDEFINED_BLOCK).getValue());
//    }
//    
//    @Test
//    public void testProjectIDNotFoundCounter() throws IOException, ParseException{
//    	int id = 116502740;
//		String src = Util.retrieveProjectOnline(id);
//    	JSONObject line = new JSONObject();
//    	line.put("_id", id);
//    	line.put("src", src);
//    	Text value = new Text(line.toJSONString());
//
//		
//        mapDriver.withInput(new LongWritable(), value) 
//                .runTest();
//        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
//                .findCounter(ErrorCounter.PROJECT_ID_NOT_FOUND).getValue());
//    }
}
