package com.example.hadoop_final_homework_202003117.config;

import com.example.hadoop_final_homework_202003117.HadoopFinalHomework202003117Application;
import com.example.hadoop_final_homework_202003117.map_reduce.WordMapper;
import com.example.hadoop_final_homework_202003117.map_reduce.WordReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ReduceJobsConfiguration {

	@Value("${hdfs.hdfsPath}")
	private String hdfsPath;

	/**
	 * 获取HDFS配置信息
	 *
	 * @return
	 */
	public Configuration getConfiguration() {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", hdfsPath);
		configuration.set("mapred.job.tracker", hdfsPath);
		return configuration;
	}

	/**
	 * 获取单词统计的配置信息
	 *
	 * @param jobName
	 * @param inputPath
	 * @param outputPath
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public void getWordCountJobsConf(String jobName, String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConfiguration();
		Job job = Job.getInstance(conf, jobName);

		job.setMapperClass(WordMapper.class);
		job.setCombinerClass(WordReduce.class);
		job.setJarByClass(HadoopFinalHomework202003117Application.class);
		job.setReducerClass(WordReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}

	public String getHdfsPath() {
		return hdfsPath;
	}
}

