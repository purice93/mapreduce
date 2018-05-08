package com.sound.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator {
	// 指明比较的对象，true表明创建对象mykey实例
	public MySort() {
		super(MyKey.class, true);
	}

	// 大小排序
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey akey = (MyKey) a;
		MyKey bkey = (MyKey) b;
		return akey.compareTo(bkey);
	}

}
