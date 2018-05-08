package com.sound.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{
	public MyGroup() {
		super(MyKey.class,true);
	}
	
	// 进行分组，即年相同、月相同，分为一组；
	// 只比较是否相等，不比较大小；大小排序有sort完成
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey akey = (MyKey) a;
		MyKey bkey = (MyKey) b;
		int r0 = Integer.compare(akey.getYear(), bkey.getYear());
		if(r0==0){
			return Integer.compare(akey.getMonth(),bkey.getMonth());
		}else {
			return r0;
		}
	}
	
}
