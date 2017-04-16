package vt.cs.analyzer.mapred;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.TimeoutException;

import main.AnalysisManager;
import main.ScratchParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import AST.Program;

//ref:http://appsintheopen.com/posts/40-unit-testing-map-reduce-programs-with-mrunit

public class AnalysisMapper extends Mapper<Object, Text, LongWritable, Text> {

	public static enum OUTCOMECOUNTER {
		SUCCESS, TIMEOUT, ANALYSIS_ERROR, MISMATCHED_PROJECT_ID, PARSING_FAILURE, PROJECT_ID_NOT_FOUND
	}

	private MultipleOutputs<LongWritable, Text> mos;
	private int timeout = 60;

	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<LongWritable, Text>(context);
		Configuration conf = context.getConfiguration();
		String analysisTimeout = conf.get("analysisTimeout");
		try {
			timeout = Integer.parseInt(analysisTimeout);
		} catch (Exception e) {
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String report = null;
		LongWritable projectID = null;

		try {
			JSONObject record = (JSONObject) new JSONParser().parse(value
					.toString());
			projectID = new LongWritable((Long) record.get("_id"));
			String src = record.get("src").toString();
			ScratchParser scratchParser = new ScratchParser();
			Program program = scratchParser.parse(new StringReader(src));
			report = AnalysisManager.processWithTimer(program, false, timeout);

			if (program.getProjectID() != projectID.get()) {
				context.getCounter(OUTCOMECOUNTER.MISMATCHED_PROJECT_ID)
						.increment(1);
			} else if (report != null) {
				Text result = new Text(report);
				mos.write("success", projectID, result);
				context.getCounter(OUTCOMECOUNTER.SUCCESS).increment(1);
			}
		} catch (TimeoutException e) {
			context.getCounter(OUTCOMECOUNTER.TIMEOUT).increment(1);
			mos.write("timeoutID", projectID, NullWritable.get());
			mos.write("timeoutSRC", NullWritable.get(), value);
		} catch (Exception e) {
			context.getCounter(OUTCOMECOUNTER.ANALYSIS_ERROR).increment(1);
			Text errorMsg = new Text(e.toString());
			mos.write("errorID", projectID, errorMsg);
			mos.write("errorSRC", NullWritable.get(), value);
		}

	}
}
