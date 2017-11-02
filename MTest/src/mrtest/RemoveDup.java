package mrtest;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RemoveDup {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
			
			Text keyToken = null; //new Text();
			Text valueToken = null; //new Text();
			
//			while (stringTokenizer.hasMoreTokens()) {
//				if (keyToken == null) {
//					keyToken = new Text();
//					keyToken.set(stringTokenizer.nextToken());
//				}
//				else if (valueToken == null) {
//					valueToken = new Text();
//					valueToken.set(stringTokenizer.nextToken());
//				}
//		    }

			keyToken = new Text(value.toString());
			context.write(keyToken, new Text(""));
		}
	}
	
	public static class OutputReduce extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//Text val = null;
			for (Text val : values) {
				//context.write(key, val);
			}
			
			context.write(key, new Text(""));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "remove dup");
	    job.setJarByClass(RemoveDup.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(OutputReduce.class);
	    job.setReducerClass(OutputReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
