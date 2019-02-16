package projectv2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import projectv2.ProvinceStatAPP.ProvinceStatMapper;
import projectv2.ProvinceStatAPP.ProvinceStatReducer;

public class DeviceAPP {

	public static void main(String[] args) throws Exception{

		//set the input/output path
				Path input_path = new Path(args[0]);//"ETL_Output"
				Path output_path = new Path(args[1]);//"Device_Output"

				Configuration conf = new Configuration();
				//if the output path already exists before running, delete it
				FileSystem fs = FileSystem.get(conf);
				if (fs.exists(output_path))
				{
					fs.delete(output_path,true);
					
				}
				
			 	
				Job job = Job.getInstance(conf);
				
				job.setJarByClass(DeviceAPP.class);
				job.setMapperClass(DeviceMapper.class);
				job.setReducerClass(DeviceReducer.class);
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(LongWritable.class);
			 
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(LongWritable.class);

				FileInputFormat.setInputPaths(job, input_path);
				FileOutputFormat.setOutputPath(job, output_path);
						
				boolean result = job.waitForCompletion(true);
				
				System.out.print("Device stat done!");
				System.exit(result ? 0 : -1);
		
		
		
	}
	
	
	public static class DeviceMapper extends Mapper<LongWritable,Text,Text,LongWritable>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if (value!=null)	
			{
			String device = value.toString().split("\t")[6];
			Text input = new Text(device);
			context.write(input, new LongWritable(1));	
			}
			else
			{context.write(new Text("-"), new LongWritable(1));	
				
			}
		}
	
	}
	
	
	public static class DeviceReducer extends Reducer<Text,LongWritable,Text,LongWritable>
	{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
			
			long count = 0 ;
			for(LongWritable value  : values)
			{
				count++;
			}
			
			context.write(key, new LongWritable(count));
			
			
		}
				
	}
}
