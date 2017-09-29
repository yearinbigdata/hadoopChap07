package com.example.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.parser.AirlinePerformanceParserBack;

public class ReduceSideJoin extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.printGenericCommandUsage(System.out);
		args = new String[] { "-fs", "hdfs://bigdata01:9000", "-jt", "bigdata01:9001", };
		ToolRunner.run(new ReduceSideJoin(), args);
	}

	// 그룹키 정의
	static class TaggedGroupKeyComparator extends WritableComparator {

		protected TaggedGroupKeyComparator() {
			super(TaggedKey.class, true);
		}

		@Override // 그룹 비교기 구현 (AA###1과 AA###0을 같은 그룹으로 만들기 위함)
		public int compare(WritableComparable a, WritableComparable b) {
			TaggedKey k1 = (TaggedKey) a;
			TaggedKey k2 = (TaggedKey) b;

			// 태그는 비교하지 말고, 코드만 같으면 같은 그룹으로 보는 로직
			return k1.getCode().compareTo(k2.getCode());
		}

	}

	// 복합키 만들기 (code + tag)
	static class TaggedKey implements WritableComparable<TaggedKey> {

		String code;
		Integer tag;

		public TaggedKey() {
		}

		public TaggedKey(String code, Integer tag) {
			this.code = code;
			this.tag = tag;
		}
		
		//hashCode정의
		@Override
		public int hashCode() {
			return Objects.hash(code, tag);
		}

		@Override
		public String toString() {
			return code + "###" + tag;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, code);
			out.writeInt(tag);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			code = WritableUtils.readString(in);
			tag = in.readInt();
		}

		@Override
		public int compareTo(TaggedKey o) {
			int cmp = code.compareTo(o.code);

			if (cmp == 0) {
				cmp = tag.compareTo(o.tag);
			}
			return cmp;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		public Integer getTag() {
			return tag;
		}

		public void setTag(Integer tag) {
			this.tag = tag;
		}
	}

	static class OntimeMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
		TaggedKey outputKey = new TaggedKey();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirlinePerformanceParserBack parser = new AirlinePerformanceParserBack(value);

			// 복합키로 하기 전
			// outputKey.set(parser.getUniqueCarrier() + ",1");

			// 복합기로 한 후
			outputKey.setCode(parser.getUniqueCarrier());
			outputKey.setTag(1);

			context.write(outputKey, value);
//			context.write(new TaggedKey(parser.getUniqueCarrier(), 1), value);

		}
	}

	static class CarrierMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
		TaggedKey outputKey = new TaggedKey();
		Text outValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			int idx = record.indexOf(",");
			String code = record.substring(0, idx);
			String name = record.substring(idx + 1);

			// outputKey.set(code + ",0");

			// 복합키
			outputKey.setCode(code);
			outputKey.setTag(0);
			outValue.set(name);

			context.write(outputKey, outValue);
//			context.write(new TaggedKey(code, 0), new Text(name));
		}
	}

	// 리듀서 정의
	static class CarrierReducer extends Reducer<TaggedKey, Text, Text, Text> {

		Text outputKey = new Text();
		Text outValue = new Text();

		@Override
		protected void reduce(TaggedKey key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {

			Text name = new Text(); // value에서 뽑아내서 carrier name 저장

			boolean first = true;
			for (Text v : value) {
				if (first) { // 처음 것만 저장
					name.set(v.toString());
					first = false;
					continue;
				}
				outputKey.set(key.getCode());	
				outValue.set(name.toString() + "\t" + v.toString());// join
				context.write(outputKey, outValue);
			}
			
			/*
			 * AA, 0	carrier name...
			 * AA, 1	1987, 10, xx, xxx, ....
			 * AA, 1	1987, 10, xx, xxx, ....
			 * AA, 1	1987, 10, xx, xxx, ....
			 */
			
			
			
			
		}

	}
	
	static class GroupPartitioner extends Partitioner<TaggedKey, Text>{

		@Override
		public int getPartition(TaggedKey key, Text value, int numpartitions) {
			
			//키가 같으면 같은 리듀스에 가도록 한다
			return Math.abs(key.getCode().hashCode() % numpartitions);
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);

		// mapper가 여러개인 경우
		MultipleInputs.addInputPath(job, new Path("/home/java/dataexpo/1987_nohead.csv"), TextInputFormat.class,
				OntimeMapper.class);
		MultipleInputs.addInputPath(job, new Path("/home/java/dataexpo_code/carriers_nohead.csv"),
				TextInputFormat.class, CarrierMapper.class);

		job.setNumReduceTasks(3);

		job.setGroupingComparatorClass(TaggedGroupKeyComparator.class);// 그룹 비교키
		job.setReducerClass(CarrierReducer.class);

		job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(GroupPartitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputDir = new Path("/home/java/reducejoin");
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(getConf());
		hdfs.delete(outputDir, true);
		hdfs.close();

		job.waitForCompletion(true);
		return 0;
	}

}