package com.sound.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;


/**
 * 
 * @author 53033 <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	// 对每一行句子进行map-split（相当于一个切分）
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String[] words = StringUtils.split(value.toString(), ' ');
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
