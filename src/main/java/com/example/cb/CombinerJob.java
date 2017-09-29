package com.example.cb;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.parser.AirlinePerformanceParser;

public class CombinerJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		args = new String[] { "-fs", "hdfs://bigdata01:9000", "-jt", "bigdata01:9001" };
		ToolRunner.run(new CombinerJob(), args);
	}

	// 연도별 출발 지연 건수 구하기
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		static final IntWritable one = new IntWritable(1);

		Text outputKey = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

			if (parser.isDepartureDelayAvailable()) {
				if (parser.getDepartureDelayTime() > 0) {
					outputKey.set(parser.getYear() + "");
					context.write(outputKey, one);
				}
			}
		}
	}

	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable v : value) {
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "Combiner");
		job.setJarByClass(CombinerJob.class);
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path outputDir = new Path("/home/java/combiner");
		FileOutputFormat.setOutputPath(job, outputDir);
		FileSystem hdfs = FileSystem.get(getConf());
		hdfs.delete(outputDir, true);
		hdfs.close();

		job.waitForCompletion(true);
		return 0;
	}

}
