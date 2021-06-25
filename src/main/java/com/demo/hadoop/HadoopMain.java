package com.demo.hadoop;

import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.demo.hadoop.service.HdfsService;

@SpringBootApplication
public class HadoopMain {
	
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		ConfigurableApplicationContext context = SpringApplication.run(HadoopMain.class, args);
		
		HdfsService service = context.getBean(HdfsService.class);
		
		service.testMkdirs(); // 创建文件夹
		service.testPut(); // 文件上传
		service.testGet(); // 文件下载
		service.testMv(); // 文件更名和移动
		service.testLs(); // 文件详情
		service.testFile(); // 判断文件和文件夹
		service.testAppend(); // 在文件中追加内容
		service.testRm(); // 文件删除
		
		System.out.println();
	}
	
}
