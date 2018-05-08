package com.sound.mr.friend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author 53033
 * 用于排序
 */
public class Fofer implements WritableComparable<Fofer>{
	private String fof;
	private int num;
	
	
	public String getFof() {
		return fof;
	}

	public void setFof(String fof) {
		this.fof = fof;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	@Override
	public int compareTo(Fofer o) {
		int r0 = -Integer.compare(this.getNum(), o.getNum());
		
		
		// 这里必须要比较字符串，否则reduce时，对于num相同的，只取其中一个
		if(r0==0) {
			return -this.getFof().compareTo(o.getFof());
		}else {
			return r0;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.getFof());
		out.writeInt(this.getNum());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.setFof(in.readUTF());
		this.setNum(in.readInt());
	}

}
