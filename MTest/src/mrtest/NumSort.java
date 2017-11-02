package mrtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Reducer;

public class NumSort {
	
	public static class NumMapper extends Mapper<Object, Text, IntWritable, Text>{
		private static IntWritable intKey = new IntWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			int intValue = Integer.parseInt(value.toString());
			intKey.set(intValue);
			context.write(intKey, new Text(""));
		}
	}
	
	public static class OutReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable>{
		private static IntWritable linenum = new IntWritable(1);
		
		public void reduce(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			linenum = new IntWritable(linenum.get() + 1);
			context.write(linenum, key);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "num sort");
	    job.setJarByClass(NumSort.class);
	    job.setMapperClass(NumMapper.class);
	    job.setCombinerClass(OutReducer.class);
	    job.setReducerClass(OutReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
