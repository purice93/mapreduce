package com.sound.mr.friend;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;


public class MainJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		// 本地调试测试
		config.set("fs.defaultFS", "hdfs://node1:8020");
		config.set("yarn.resourcemanager.hostname", "node1");

		// 连接服务器测试
		// config.set("mapred.jar", "C:\\Users\\53033\\Desktop\\wc.jar");
		
		
		// 之所以有两个是因为，第一个用于找到所有的fof关系
		// 第二个是对fof关系进行排序
		if(runFind(config)){
			runSort(config);
		}
	}

	public static void run2(Configuration config) {
		try {
			FileSystem fs =FileSystem.get(config);
			Job job =Job.getInstance(config);
			job.setJarByClass(MainJob.class);
			
			job.setJobName("fof2");
			
			job.setMapperClass(SortedMapper.class);
			job.setReducerClass(SortedReducer.class);
			job.setSortComparatorClass(FoFSort.class);
			job.setGroupingComparatorClass(FoFGroup.class);
			job.setMapOutputKeyClass(User.class);
			job.setMapOutputValueClass(User.class);
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			//设置MR执行的输入文件
			FileInputFormat.addInputPath(job, new Path("/usr/output/friend"));
			
			//该目录表示MR执行之后的结果数据所在目录，必须不能存在
			Path outputPath=new Path("/usr/output/f2");
			if(fs.exists(outputPath)){
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			
			boolean f =job.waitForCompletion(true);
			if(f){
				System.out.println("job 成功执行");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	
	// 对所有的fof关系进行大小排序
	private static void runSort(Configuration config) {
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(MainJob.class);
			job.setJobName("sort fof of friends");

			job.setMapOutputKeyClass(Fofer.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setMapperClass(SortMapper.class);
			job.setReducerClass(SortReducer.class);
			
			job.setSortComparatorClass(MySort.class);
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			FileSystem fs = FileSystem.get(config);
//			Path inPath = new Path("/usr/input/");
//			if (!fs.exists(inPath)) {
//				fs.mkdirs(inPath);
//			}
			FileInputFormat.addInputPath(job, new Path("/usr/output/friend"));

			Path outPath = new Path("/usr/output/friendSort");
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

	//	用于查找所有Fof关系	
	public static boolean runFind(Configuration config) {
		boolean finished = false;
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(MainJob.class);
			job.setJobName("find fof of friends");

			job.setMapOutputKeyClass(Fof.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setMapperClass(FofMapper.class);
			job.setReducerClass(FofReducer.class);
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			FileSystem fs = FileSystem.get(config);
			Path inPath = new Path("/usr/input/");
			if (!fs.exists(inPath)) {
				fs.mkdirs(inPath);
			}
			FileInputFormat.addInputPath(job, new Path("/usr/input/"));

			Path outPath = new Path("/usr/output/friend");
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}
			FileOutputFormat.setOutputPath(job, outPath);

			finished = job.waitForCompletion(true);

			if (finished) {
				System.out.println("finished success!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return finished;
	}

	static class FofMapper extends Mapper<Text, Text, Fof, IntWritable> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String user = key.toString();
			String[] friends = StringUtils.split(value.toString(), '\t');
			for (int i = 0; i < friends.length; i++) {
				Fof ofof = new Fof(user, friends[i]);
				
				// 朋友关系
				context.write(ofof, new IntWritable(0));
				for(int j=i+1;j<friends.length;j++) {
					// fof关系，但是也可能是朋友关系
					Fof fof = new Fof(friends[i],friends[j]);
					context.write(fof, new IntWritable(1));
				}
			}
		}

	}
	
	static class FofReducer extends Reducer<Fof, IntWritable, Fof, IntWritable>{

		@Override
		protected void reduce(Fof arg0, Iterable<IntWritable> arg1,
				Context arg2) throws IOException, InterruptedException {
			int sum =0;
			boolean isFriend = false;
			for(IntWritable iw: arg1) {
				if(iw.get()==0) {
					isFriend = true;
					break; // 等于0，表示已经是朋友关系，就不用累计计算了
				}else {
					sum+=iw.get();
//					sum+=1;
				}
			}
			if(!isFriend){
				arg2.write(arg0, new IntWritable(sum));
			}
		}
		
	}
	
	
	static class SortMapper extends Mapper<Text, Text, Fofer, NullWritable> {
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// 这里注意一个情况，第一步run输出的文件，第一个间隔符在fof中间，
			// 所以key值为一个单个人名：“老王”；value为：“老宋  3”
			String[] fofs = StringUtils.split(value.toString(), '\t');
			int num = Integer.valueOf(fofs[1]);
			Fofer fofer = new Fofer();
			fofer.setFof(key.toString()+'\t'+fofs[0]);
			fofer.setNum(num);
			context.write(fofer, NullWritable.get());
		}

	}
	
	static class SortReducer extends Reducer<Fofer, NullWritable, Text, NullWritable>{

		@Override
		protected void reduce(Fofer arg0, Iterable<NullWritable> arg1,
				Context arg2) throws IOException, InterruptedException {
			// 直接写入
			arg2.write(new Text(arg0.getFof()+arg0.getNum()), NullWritable.get());
		}
		
	}
	
static class SortedMapper extends Mapper<Text, Text, User, User>{
		
		protected void map(Text key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String[] args=StringUtils.split(value.toString(),'\t');
			String other=args[0];
			int friendsCount =Integer.parseInt(args[1]);
			
			context.write(new User(key.toString(),friendsCount), new User(other,friendsCount));
			context.write(new User(other,friendsCount), new User(key.toString(),friendsCount));
		}
	}
	
	static class SortedReducer extends Reducer<User, User, Text, Text>{
		protected void reduce(User arg0, Iterable<User> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			String user =arg0.getUname();
			StringBuffer sb =new StringBuffer();
			for(User u: arg1 ){
				sb.append(u.getUname()+":"+u.getFriendsCount());
				sb.append(",");
			}
			arg2.write(new Text(user), new Text(sb.toString()));
		}
	}
}
