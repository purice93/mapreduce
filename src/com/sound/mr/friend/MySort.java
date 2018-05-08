package com.sound.mr.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 
 * @author 53033
 * 排序规则
 */
public class MySort extends WritableComparator {
	// 指明比较的对象，true表明创建对象mykey实例
	public MySort() {
		super(Fofer.class, true);
	}

	// 大小排序
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Fofer akey = (Fofer) a;
		Fofer bkey = (Fofer) b;
		return akey.compareTo(bkey);
	}

}
