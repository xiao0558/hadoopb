package com.demo.hadoop.config;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.PreDestroy;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FileSystemConfig {

	private FileSystem fs;
	
	// 配置文件优先级：Configuration > 开发环境hdfs-site.xml > 服务器上的hdfs-site.xml > 服务器上的hdfs-default.xml
	@Bean
	public FileSystem fileSystem() throws URISyntaxException, IOException, InterruptedException {
		URI uri = new URI("hdfs://hadoop76:8020");
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		configuration.set("dfs.replication", "2");
		String user = "root";
		fs = FileSystem.get(uri, configuration, user);
		return fs;
	}
	
	@PreDestroy
    public void preDestroy() throws IOException {
		fs.close();
    }
	
}
