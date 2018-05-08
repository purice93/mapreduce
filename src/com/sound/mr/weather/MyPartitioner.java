package com.sound.mr.weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, DoubleWritable>{
	
	// 根据年进行分组，即一年对应一个reduce
	@Override
	public int getPartition(MyKey key, DoubleWritable value, int numReduceTasks) {
		// 1949为最小年份，除以reduce数目，取余
		return (key.getYear()-1949)%numReduceTasks;
	}
	
}
