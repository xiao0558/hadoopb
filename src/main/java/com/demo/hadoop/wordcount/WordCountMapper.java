package com.demo.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text k = new Text();
	IntWritable v = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1 获取一行
		// mapper输入：索引值，文本
		// key:0,value:zhangsan zhangsan
		String line = value.toString();
		// 2 切割
		String[] words = line.split(" ");
		// 3 输出
		// key:1 把每个单词都标记为1，包括相同的单词
		for (String word : words) {
			k.set(word);
			context.write(k, v);
			// mapper输出：文本，个数 
			// zhangsan,1 zhangsan,1
			System.out.println("map -> " + k.toString() + ":" + v.toString());
		}
	}

}
