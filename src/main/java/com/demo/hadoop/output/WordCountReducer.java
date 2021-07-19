package com.demo.hadoop.output;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		// 获取一行
		// Reducer输入：zhangsan
		for (NullWritable nullval : values) {
			// 2 输出
			context.write(key, nullval);
			// Reducer输出：zhangsan
			System.out.println("reduce -> " + key.toString() + ":" + nullval);
		}
	}

}
