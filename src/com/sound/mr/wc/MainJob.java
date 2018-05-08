package com.sound.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
//		config.set("fs.defaultFS", "hdfs://node1:8020");
//		config.set("yarn.resourcemanager.hostname", "node1");
		
		config.set("mapred.jar", "C:\\Users\\53033\\Desktop\\wc.jar");
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(MainJob.class);
			job.setJobName("WordCount");

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			FileSystem fs = FileSystem.get(config);
			Path inPath = new Path("/usr/input/");
			if (!fs.exists(inPath)) {
				fs.mkdirs(inPath);
			}
			FileInputFormat.addInputPath(job, new Path("/usr/input/"));

			Path outPath = new Path("/usr/output/wc");
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}
			FileOutputFormat.setOutputPath(job, outPath);

			boolean finished = job.waitForCompletion(true);

			if (finished) {
				System.out.println("finished success!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
