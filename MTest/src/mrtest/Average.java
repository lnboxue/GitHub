package mrtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Average {
	
	public static class AddMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private Text nameKey = new Text();
		private IntWritable nameInt = new IntWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] keyValue = line.split(" ");
			int nint = Integer.parseInt(keyValue[1]);
			System.out.println("keyValue: " + keyValue[0] +"/"+ nint);
			nameKey.set(keyValue[0]);
			nameInt.set(nint);
			context.write(nameKey, nameInt);
			
		}
	}
	
	public static class OutReducer extends Reducer<Text, IntWritable, Text, Text>{
		
		private Text avg = new Text();
		
		public void reduce(Text key, Iterable<IntWritable> notes, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int counter = 0;
			for (IntWritable note : notes) {
				sum += note.get();
				counter ++;
			}
			
			String average = Double.toString(sum / counter);
			avg.set(average);
			context.write(key, avg);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word avg");
	    job.setJarByClass(Average.class);
	    job.setMapperClass(AddMapper.class);
	    job.setCombinerClass(OutReducer.class);
	    job.setReducerClass(OutReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
