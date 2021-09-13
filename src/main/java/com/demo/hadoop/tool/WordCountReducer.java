package com.demo.hadoop.tool;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	int sum;
	IntWritable v = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// 获取一行
		// Reducer输入：文本，个数数组 
		// zhangsan :(1,1)
		// 1 累加求和，计算每个单词出现的次数
		sum = 0;
		for (IntWritable count : values) {
			sum += count.get();
		}
		// 2 输出
		v.set(sum);
		context.write(key, v);
		// Reducer输出：文本，数量 
		// zhangsan,2
		System.out.println("reduce -> " + key.toString() + ":" + v.toString());
	}

}
