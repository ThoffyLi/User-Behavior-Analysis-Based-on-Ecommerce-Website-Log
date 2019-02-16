package project;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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

import IPParser.LogParser;
import project.PVStatAPP.PVStatMapper;
import project.PVStatAPP.PVStatReducer;

public class LogParseAPP {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	 	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(LogParseAPP.class);
		job.setMapperClass(LogParseMapper.class);
		job.setReducerClass(LogParseReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
	 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path("Pinput"));
		FileOutputFormat.setOutputPath(job, new Path("Output"));
				
		boolean result = job.waitForCompletion(true);
		
		System.out.print("OK!!!!");
		System.exit(result ? 0 : -1);
		
 
	}	
	
	
	
	public static class LogParseMapper extends Mapper<LongWritable,Text,Text,LongWritable>
	
	  {
		
	    @Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			 if (value!=null)
			 {
 
		     LogParser parser = new LogParser();
	    	 Map<String,String> map =  parser.ParseLog(value.toString());
	    	 if  (StringUtils.isNotBlank(map.get("province")))
	    	 { 
	    	 Text province = new Text(map.get("province"));
	    	 context.write(province, new LongWritable(1));
	    	 }
	    	 else
	    	 {
	    		 context.write(new Text("-"), new LongWritable(1));
	    		 
	    	 }
			 }    	
		}	
	  }

	public static class LogParseReducer extends Reducer<Text,LongWritable,Text,LongWritable>
	{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				 Context context) throws IOException, InterruptedException {

              long count = 0; 			
		      for(LongWritable value : values) 
		      {
		    	  count++;
		      }
			
		      context.write(key, new LongWritable(count));
			
			
		}
		
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
}


