package com.sound.mr.friend;

import org.apache.hadoop.io.Text;

public class Fof extends Text {
	public Fof() {
		super();
	}
	
	public Fof(String a,String b){
		super(getFof(a,b));
	}

	// 对fof关系进行排序，即：小明-小红，小红-小明统一为一个
	private static String getFof(String a, String b) {
		int r0 = a.compareTo(b);
		if(r0<0){
			return a+"\t"+b;
		}else {
			return b+"\t"+a;
		}
	}
}
