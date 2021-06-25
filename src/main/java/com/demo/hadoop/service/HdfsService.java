package com.demo.hadoop.service;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.RandomUtil;

@Service
public class HdfsService {

	@Autowired
	private FileSystem fs;
	
	// 创建文件夹
	public void testMkdirs() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/test")); // 创建文件夹
	}

	// 文件上传
	public void testPut() throws IllegalArgumentException, IOException {
		File tempFile = new File(System.getProperty("java.io.tmpdir") + "bbb.txt");
		FileUtil.writeUtf8String(RandomUtil.randomString(10), tempFile);
		Path src = new Path(tempFile.getPath());
		Path dst = new Path("/test");
		fs.copyFromLocalFile(true, true, src, dst); // 文件上传，并删除本地文件，覆盖服务器上的文件
	}

	// 文件下载
	public void testGet() throws IllegalArgumentException, IOException {
		String tmpdir = System.getProperty("java.io.tmpdir");
		FileUtil.del(tmpdir + "/test"); // 删除本地已存在的文件夹
		Path dst = new Path(tmpdir);
		Path src = new Path("/test");
		fs.copyToLocalFile(false, src, dst, true); // 下载文件夹，不删除远程文件，下载后不进行crc校验
		Runtime.getRuntime().exec("explorer.exe " + dst.toString().replace("/", "\\") + "\\test");
	}

	// 文件更名和移动
	public void testMv() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/test1")); // 创建文件夹
		fs.rename(new Path("/test/bbb.txt"), new Path("/test1/aaa.txt")); // 开始更名和移动
	}

	// 文件详情
	public void testLs() throws IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus lfs = listFiles.next();
			System.out.println("========== " + lfs.getPath() + " ==========");
			System.out.println(lfs.getPermission());
			System.out.println(lfs.getOwner());
			System.out.println(lfs.getGroup());
			System.out.println(lfs.getLen());
			System.out.println(lfs.getModificationTime());
			System.out.println(lfs.getReplication());
			System.out.println(lfs.getBlockSize());
			System.out.println(lfs.getPath().getName());

			BlockLocation[] blockLocations = lfs.getBlockLocations(); //
			System.out.println(Arrays.toString(blockLocations));
		}
	}

	// 判断文件和文件夹
	public void testFile() throws IllegalArgumentException, IOException {
		FileStatus[] listStatus = fs.listStatus(new Path[] { new Path("/"), new Path("/test1") });
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isFile()) {
				System.out.println("文件：" + fileStatus.getPath().getName());
			} else {
				System.out.println("目录：" + fileStatus.getPath().getName());
			}
		}
	}
	
	// 在文件中追加内容
	public void testAppend() throws IllegalArgumentException, IOException {
		File tempFile = new File(System.getProperty("java.io.tmpdir") + "bbb.txt");
		FileUtil.writeUtf8String(new Date().toString() + "中文", tempFile);
		Path src = new Path(tempFile.getPath());
		Path dst = new Path("/test");
		fs.copyFromLocalFile(true, true, src, dst); // 文件上传，并删除本地文件，覆盖服务器上的文件
		// 文件追加内容
		FSDataOutputStream append = fs.append(new Path("/test/bbb.txt"));
		for (int i = 0; i < 5; i++) {
			append.writeUTF(new Date().toString() + "中文");
			append.hflush();
		}
		append.close();
	}
	
	// 文件删除
	public void testRm() throws IllegalArgumentException, IOException {
		// 开始删除
		fs.delete(new Path("/test1/aaa.txt"), false); // 删除文件
		fs.delete(new Path("/test"), true); // 删除目录，非空目录参数是true
		fs.delete(new Path("/test1"), true); // 删除目录
	}
	
}
