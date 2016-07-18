package vt.cs.analyzer.mapred;

import java.io.IOException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import vt.cs.smells.analyzer.AnalysisException;
import vt.cs.smells.analyzer.AnalysisManager;
import vt.cs.smells.analyzer.nodes.ScratchProject;
import vt.cs.smells.analyzer.parser.ParsingException;
import vt.cs.smells.analyzer.parser.ProjectIDNotFoundException;
import vt.cs.smells.analyzer.parser.UndefinedBlockException;

//ref:http://appsintheopen.com/posts/40-unit-testing-map-reduce-programs-with-mrunit

public class AnalysisMapper extends Mapper<Object, Text, LongWritable, Text> {

	public static enum ErrorCounter {
		MISMATCHED_PROJECT_ID, ANALYSIS_FAILURE, PARSING_FAILURE, UNDEFINED_BLOCK, PROJECT_ID_NOT_FOUND
	};

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		AnalysisManager blockAnalyzer = new AnalysisManager();
		JSONObject report = null;
		LongWritable projectID = null;

		try {
			JSONObject record = (JSONObject) new JSONParser().parse(value
					.toString());
			projectID = new LongWritable((Long) record.get("_id"));
			String src = record.get("src").toString();
			ScratchProject project = ScratchProject.loadProject(src);
			blockAnalyzer.analyze(project);
			report = blockAnalyzer.getConciseJSONReports();

			if (blockAnalyzer.getProjectID() != projectID.get()) {
				context.getCounter(ErrorCounter.MISMATCHED_PROJECT_ID)
						.increment(1);
			} else if (report != null) {
				Text result = new Text(report.toJSONString());
				context.write(projectID, result);
			}
		} catch (ParsingException e) {
			context.getCounter(ErrorCounter.PARSING_FAILURE).increment(1);
			if (ExceptionUtils.getRootCause(e) instanceof UndefinedBlockException) {
				context.getCounter(ErrorCounter.UNDEFINED_BLOCK).increment(1);
			} else if (ExceptionUtils.getRootCause(e) instanceof ProjectIDNotFoundException) {
				context.getCounter(ErrorCounter.PROJECT_ID_NOT_FOUND)
						.increment(1);
			} else if (ExceptionUtils.getRootCause(e) instanceof NullPointerException){
				
			} else {
				e.printStackTrace();
			}
		} catch (AnalysisException e) {
			context.getCounter(ErrorCounter.ANALYSIS_FAILURE).increment(1);
			System.err.println("=======>"+projectID);
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
