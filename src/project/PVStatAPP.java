package project;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PVStatAPP {
	
public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
 	
	Job job = Job.getInstance(conf);
	
	job.setJarByClass(PVStatAPP.class);
	job.setMapperClass(PVStatMapper.class);
	job.setReducerClass(PVStatReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
 
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(LongWritable.class);

	FileInputFormat.setInputPaths(job, new Path("Pinput"));
	FileOutputFormat.setOutputPath(job, new Path("output"));
			
	boolean result = job.waitForCompletion(true);
	
	System.out.print("OK!!!!");
	System.exit(result ? 0 : -1);
	
}
	
	
public static class PVStatMapper extends Mapper<LongWritable,Text,Text,LongWritable>
{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 
	    final Text KEY = new Text ("KEY");
	    final LongWritable ONE = new LongWritable(1);
	    
		context.write(KEY, ONE);
		
	}
	
}


public static class PVStatReducer extends Reducer<Text, LongWritable,NullWritable,LongWritable>

{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, NullWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
	 
		
		long count = 0;
		 
		for (LongWritable value : values)
		{       count++;         }
		
		context.write(NullWritable.get(), new LongWritable(count));
		
	}
	
	
	

}






	

}
