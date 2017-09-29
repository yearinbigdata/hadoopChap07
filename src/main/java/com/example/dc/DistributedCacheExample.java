package com.example.dc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import com.example.parser.AirlinePerformanceParserBack;

public class DistributedCacheExample extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.printGenericCommandUsage(System.out);
		
		args = new String[] {
				"-fs", "hdfs://bigdata01:9000", 
				"-jt", "bigdata01:9001",
				"-files", "/project/02_Software/dataexpo/code/carriers_nohead.csv#carriers"
																				//링크 걸기(별칭처럼)
		};
		
		ToolRunner.run(new DistributedCacheExample(), args);	//분산캐시 자동 업로드까지 해줌
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		Map<String, String> map = new HashMap<>();
		
		
		@Override	//한번만 접근하면 되기 때문에 초기화를 하는 setup에서 접근한다
		protected void setup(Context context) throws IOException, InterruptedException {

			//복사한 곳의 위치를 path로 알려줘라
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			//잘 읽히는지 시험삼아 출력
			System.out.println(files[0].toString());
			
			
//			FileReader reader = new FileReader(files[0].toString());
			FileReader reader = new FileReader("carriers");	
			//toolRunner를 사용할때 경로 따로 잡지 않아도 됨. 링크 이름도 따로 잡을 수 있다
			BufferedReader buffer = new BufferedReader(reader);
			
			String line = null;
			while((line=buffer.readLine()) != null){ //맵에 저장
				int idx = line.indexOf(",");	//컴마의 위치를 리턴. 첫 번째 컴마 이전을 키로, 그 이후를 코드로 삼음
				String key = line.substring(0, idx);
				String value = line.substring(idx+1);
				map.put(key, value);
			}
			
			for(Entry<String, String> e : map.entrySet()){
				System.out.println(e.getKey() + "=" + e.getValue());
			}
		
		}
		
		Text outputKey = new Text();
		static final IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirlinePerformanceParserBack parser = new AirlinePerformanceParserBack(value);
			
		outputKey.set(map.get(parser.getUniqueCarrier()));
		//맵에서 키를 가지고 조회한 디스크립션. 
		context.write(outputKey, value);//조인
			
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//분산캐시 등록
//		DistributedCache.addCacheFile(new Path("/home/java/dataexpo_code/carriers_nohead.csv").toUri(), getConf());
//		DistributedCache.addCacheFile(new URI("/home/java/dataexpo_code/carriers_nohead.csv"), getConf());
		// 태스크 트래커가 맵퍼, 리듀스 돌리기 전에 태스크 트래커가 있는 파일시스템으로 복사한다.
		//맵퍼, 리듀서는 이 복사된 파일을 로컬파일을 읽듯이 읽을 수 있다.
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(DistributedCacheExample.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);	//identityMapper
		job.setNumReduceTasks(0);//no reducer
//		job.setReducerClass();	//identityReducer
		
		Path outputDir = new Path("/home/java/cache");
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileSystem hdfs = FileSystem.get(getConf());
		hdfs.delete(outputDir, true);
		
		
		job.waitForCompletion(true);
		return 0;
	}

}
