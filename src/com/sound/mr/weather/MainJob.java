package com.sound.mr.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		// 本地调试测试
		 config.set("fs.defaultFS", "hdfs://node1:8020");
		 config.set("yarn.resourcemanager.hostname", "node1");

		// 连接服务器测试
//		config.set("mapred.jar", "C:\\Users\\53033\\Desktop\\wc.jar");
		try {
			Job job = Job.getInstance(config); // 加载配置
			job.setJarByClass(MainJob.class); // 运行类 // 第1个类
			job.setJobName("weather"); // 作业名

			// map输出格式
			job.setMapOutputKeyClass(MyKey.class); // 第2个类
			job.setMapOutputValueClass(DoubleWritable.class);

			// map\reduce主类
			job.setMapperClass(WeatherMapper.class); // 第3个类
			job.setReducerClass(WeatherReduce.class); // 第4个类
			
			// 分别加载reduce分区类（多少个reduce）、自定义排序类、自定义reduce分组类（reduce计算业务组，和Partitioner不同）
			job.setPartitionerClass(MyPartitioner.class); // 第5个类
			job.setSortComparatorClass(MySort.class); // 第6个类
			job.setGroupingComparatorClass(MyGroup.class); // 第7个类

			// reduce任务个数
			job.setNumReduceTasks(3);
			
			// 设置输入格式，否则报错
			// 即对于：1949-10-01 14:21:02	34c
			// 以第一个分隔符（34c前面的tab键）作为分解
			// java.lang.ClassCastException: org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.Text
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			FileSystem fs = FileSystem.get(config);
//			Path inPath = new Path("/usr/input/weather");
//			if (!fs.exists(inPath)) {
//				fs.mkdirs(inPath);
//			}
			FileInputFormat.addInputPath(job, new Path("/usr/input/weather"));

			Path outPath = new Path("/usr/output/weather");
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

	/**
	 * 
	 * @author 53033
	 * mapper内部类
	 * 解析文本数据，便于处理计算
	 */
	static class WeatherMapper extends Mapper<Text, Text, MyKey, DoubleWritable> {
		SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd HH:mm:ss");

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				/**
				 * Date和Calendar区别:
				 * 时间和日历的区别，Date是整个时间，便于计算时分秒，但是Calendar不能
				 * Calendar用于计算年月日，此时date需要转换
				 */
				Date date = sdf.parse(key.toString());
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date);
				int year = calendar.get(Calendar.YEAR);
				int month = calendar.get(Calendar.MONDAY);
				double weather = Double.parseDouble(value.toString().substring(0, value.toString().lastIndexOf('c')));
				MyKey k = new MyKey();
				k.setYear(year);
				k.setMonth(month);
				k.setWeather(weather);
				context.write(k, new DoubleWritable(weather));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 
	 * @author 53033
	 * reduce内部类
	 * 输出最终结
	 */
	static class WeatherReduce extends Reducer<MyKey, DoubleWritable, Text, NullWritable> {

		@Override
		protected void reduce(MyKey arg0, Iterable<DoubleWritable> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			int i = 0;
			for (DoubleWritable dw : arg1) {
				i++;
				String msg = arg0.getYear() + "\t" + arg0.getMonth() + "\t" + dw.get();
				arg2.write(new Text(msg), NullWritable.get());
				if (i == 3) {
					break;
				}
			}
		}

	}
}
