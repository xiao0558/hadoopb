package com.demo.hadoop.combine;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// 1 获取配置信息以及获取 job 对象
		Configuration conf = new Configuration();
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
		
		// 设置输入处理类
		job.setInputFormatClass(CombineTextInputFormat.class);
		// 设置虚拟切片大小
		// 目前文件有6个每个大小88，88*6=528，定义虚拟切片大小为200，切割出3片  
		CombineTextInputFormat.setMaxInputSplitSize(job, 200);
		
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop161:8020/hadoopb_input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop161:8020/hadoopb_output"));
		
		// 7 提交 job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
	
}
