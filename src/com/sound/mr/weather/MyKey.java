package com.sound.mr.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.sun.org.apache.regexp.internal.recompile;

public class MyKey implements WritableComparable<MyKey>{

	private int year;
	private int month;
	private double weather;
	
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public double getWeather() {
		return weather;
	}

	public void setWeather(double weather) {
		this.weather = weather;
	}

	@Override
	public int compareTo(MyKey o) {
		int r0 = Integer.compare(this.getYear(), o.getYear());
		if(r0==0) {
			int r1 = Integer.compare(this.getMonth(), o.getMonth());
			if(r1==0) {
				return Double.compare(this.getWeather(), o.getWeather());
			}else {
				return r1;
			}
		}else {
			return r0;
		}
	}

	// 反序列化：从文件中读取对象
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.year=arg0.readInt();
		this.month=arg0.readInt();
		this.weather=arg0.readDouble();
	}

	// 序列化：讲对象写入文件
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(year);
		arg0.writeInt(month);
		arg0.writeDouble(weather);
	}
	
}
