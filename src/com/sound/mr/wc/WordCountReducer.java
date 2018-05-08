package com.sound.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	// key唯一，将数字相加
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		 int sum =0 ;
		 for(IntWritable iw : arg1) {
			 sum+=iw.get();
		 }
		 arg2.write(arg0, new IntWritable(sum));
	}

}
