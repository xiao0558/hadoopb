package com.demo.hadoop.tool;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver {
	
	private static Tool tool;
	
	// 空参
	// /hadoopb_input /hadoopb_output
	// -D mapreduce.job.queuename=root.dev
	// -D mapreduce.job.queuename=root.dev /hadoopb_input /hadoopb_output
	public static void main(String[] args) throws Exception {
		// 1 获取配置信息以及获取 job 对象
		Configuration conf = new Configuration();
		
		// 创建执行程序
		tool = new WordCountTool();
		
		// 参数解析
		if (args.length > 0 && args[0].equals("-D")) {
			// 第一个参数是-D
			String[] split = args[1].split("=");
			conf.set(split[0], split[1]);
			// 执行程序
			int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 2, args.length));
			System.exit(run);
		} else {
			// 没有参数，或者第一个参数不是-D
			// 执行程序
			int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 0, args.length));
			System.exit(run);
		}
		
	}
	
}
