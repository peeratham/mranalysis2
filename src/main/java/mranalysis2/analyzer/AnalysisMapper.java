package mranalysis2.analyzer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import cs.vt.analysis.analyzer.AnalysisManager;
import cs.vt.analysis.analyzer.analysis.AnalysisException;
import cs.vt.analysis.analyzer.parser.ParsingException;

//ref:http://appsintheopen.com/posts/40-unit-testing-map-reduce-programs-with-mrunit

public class AnalysisMapper extends Mapper<Object, Text, LongWritable, Text> {

	public static enum ErrorCounter {
		MISMATCHED_PROJECT_ID, ANALYSIS_FAILURE, PARSING_FAILURE
	};
	

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		AnalysisManager blockAnalyzer = new AnalysisManager();
		JSONObject report = null;
		LongWritable projectID = null;

		try {
			JSONObject record = (JSONObject) new JSONParser().parse(value.toString());
			projectID = new LongWritable((Long) record.get("_id"));
			String src = record.get("src").toString();
			report = blockAnalyzer.analyze(src);
			
			if (blockAnalyzer.getProjectID() != projectID.get()) {
				context.getCounter(ErrorCounter.MISMATCHED_PROJECT_ID).increment(1);
			} else if(report!=null) {
				Text result = new Text(report.toJSONString());
				context.write(projectID, result);
			}
		} catch (ParsingException e) {
			context.getCounter(ErrorCounter.PARSING_FAILURE).increment(1);
			e.printStackTrace();
		} catch (AnalysisException e) {
			context.getCounter(ErrorCounter.ANALYSIS_FAILURE).increment(1);
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
