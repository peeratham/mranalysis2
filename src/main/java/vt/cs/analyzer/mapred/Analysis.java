package vt.cs.analyzer.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Analysis extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		String analysisTimeout;
		if(args.length >2){
			analysisTimeout = args[2];
		}else{
			analysisTimeout = "60";
		}

		// Create configuration
		Configuration conf = new Configuration(true);
		conf.set("analysisTimeout", analysisTimeout);

		// Create job
		Job job = Job.getInstance(conf, "Analysis");
		job.setJarByClass(getClass());

		// Setup MapReduce
		job.setMapperClass(AnalysisMapper.class);
		// job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(0);

		// Specify key / value
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// job.getConfiguration().set("mapreduce.output.basename",
		// inputPath.getName());

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "success", TextOutputFormat.class,
				LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "errorSRC",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "errorID", TextOutputFormat.class,
				LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "timeoutSRC",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "timeoutID",
				TextOutputFormat.class, LongWritable.class, NullWritable.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int result = job.waitForCompletion(true) ? 0 : 1;

		return result;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Analysis(), args);
		System.exit(exitCode);
	}

}
