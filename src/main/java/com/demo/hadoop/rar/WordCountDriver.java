package com.demo.hadoop.rar;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// 1 获取配置信息以及获取 job 对象
		Configuration conf = new Configuration();
		
		// 开启 map 端输出压缩
		conf.setBoolean("mapreduce.map.output.compress", true);
		// 设置 map 端输出压缩方式
		conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
		
		Job job = Job.getInstance(conf);
		
		// 2 关联本 Driver 程序的 jar
		job.setJarByClass(WordCountDriver.class);
		
		// 3 关联 Mapper 和 Reducer 的 jar
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 4 设置 Mapper 输出的 kv 类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 5 设置Reducer输出 kv 类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop161:8020/hadoopb_input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop161:8020/hadoopb_output"));
		
		// 设置 reduce 端输出压缩开启
		FileOutputFormat.setCompressOutput(job, true);
		// 设置压缩的方式
		 FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		// FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		
		// 7 提交 job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
	
}
