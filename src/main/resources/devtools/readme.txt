源文件
touch /tmp/1.txt
echo zhangsan zhangsan>>/tmp/1.txt
echo lisi lisi>>/tmp/1.txt
echo aaa ccc>>/tmp/1.txt
echo bbb>>/tmp/1.txt
echo ccc>>/tmp/1.txt
cat /tmp/1.txt

上传源文件
hadoop fs -mkdir /hadoopb_input
hadoop fs -put /tmp/1.txt /hadoopb_input

删除输出目录
hadoop fs -rm -r /hadoopb_output

执行任务
put D:\workspace_src\hadoop-demo\hadoopb\target\hadoopb-0.0.1-SNAPSHOT-jar-with-dependencies.jar
hadoop jar hadoopb-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.demo.hadoop.wordcount.WordCountDriver /hadoopb_input /hadoopb_output

HDFS文件系统
http://hadoop76:9870/explorer.html

查看任务和日志
http://hadoop77:8088/cluster
