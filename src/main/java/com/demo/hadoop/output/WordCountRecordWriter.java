package com.demo.hadoop.output;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WordCountRecordWriter extends RecordWriter<Text, NullWritable> {
	
	private FSDataOutputStream namelog;
	private FSDataOutputStream otherlog;

	public WordCountRecordWriter(TaskAttemptContext job) {
		try {
			URI uri = new URI("hdfs://hadoop161:8020");
			String user = "root";
			FileSystem fs = FileSystem.get(uri, job.getConfiguration(), user);
			namelog = fs.create(new Path("/hadoopb_output/namelog.log"));
			otherlog = fs.create(new Path("/hadoopb_output/otherlog.log"));
		} catch (IOException | URISyntaxException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		String log = key.toString();
		if (log.equals("zhangsan") || log.equals("lisi")) {
			namelog.writeBytes(log + "\n");
		} else {
			otherlog.writeBytes(log + "\n");
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		IOUtils.closeStream(namelog);
		IOUtils.closeStream(otherlog);
	}

}
