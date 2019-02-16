package projectv2;
 
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import IPParser.LogParserProvince;
import project.PVStatAPP.PVStatMapper;
import project.PVStatAPP.PVStatReducer;

public class ProvinceStatAPP {

	public static void main(String[] args) throws Exception {
		
		//set the input/output path
		Path input_path = new Path(args[0]);
		Path output_path = new Path(args[1]);

		Configuration conf = new Configuration();
		//if the output path already exists before running, delete it
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output_path))
		{
			fs.delete(output_path,true);
			
		}
		
	 	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ProvinceStatAPP.class);
		job.setMapperClass(ProvinceStatMapper.class);
		job.setReducerClass(ProvinceStatReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
	 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, input_path);
		FileOutputFormat.setOutputPath(job, output_path);
				
		boolean result = job.waitForCompletion(true);
		
		System.out.print("Province stat done!");
		System.exit(result ? 0 : -1);
		
	}	
	

	public static class ProvinceStatMapper extends Mapper<LongWritable,Text,Text,LongWritable>
	
	  {
		
	    @Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			 if (value!=null)
			 {
 
			 LogParserProvince parser = new LogParserProvince();
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

	public static class ProvinceStatReducer extends Reducer<Text,LongWritable,Text,LongWritable>
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


