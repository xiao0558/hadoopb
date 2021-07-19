package com.demo.hadoop.output;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1 获取一行
		// mapper输入：索引值，文本
		// key:0,value:  zhangsan zhangsan
		String line = value.toString();
		// 2 切割
		String[] words = line.split(" ");
		// 3 输出
		for (String word : words) {
			k.set(word);
			context.write(k, NullWritable.get());
			// mapper输出：zhangsan
			System.out.println("map -> " + k.toString() + ":" + NullWritable.get());
		}
	}

}
