package com.demo.hadoop.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountTool implements Tool {

	private Configuration conf;
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
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
		
		if (args.length == 2) {
			// 6 设置输入和输出路径
			FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop161:8020/" + args[0]));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop161:8020/" + args[1]));
		} else {
			// 6 设置输入和输出路径
			FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop161:8020/hadoopb_input"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop161:8020/hadoopb_output"));
		}

		// 7 提交 job
		boolean result = job.waitForCompletion(true);
		return result ? 0 : 1;
	}

}
