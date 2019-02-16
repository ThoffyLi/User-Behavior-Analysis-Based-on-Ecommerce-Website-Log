// Firstly, using Mapper to conduct ETL on the log to get rid off noises and formatize data

package projectv2;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import IPParser.LogParser;

public class ETLApp {
	
	
	public static void main(String[] args) throws Exception{
		
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
		
		job.setJarByClass(ETLApp.class);
		job.setMapperClass(ETLMapper.class);
	 
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
	

		FileInputFormat.setInputPaths(job, input_path);
		FileOutputFormat.setOutputPath(job, output_path);
				
		boolean result = job.waitForCompletion(true);
		
		System.out.println("ETL done!");
		System.exit(result ? 0 : -1);
		
		
	}
	
	
	public static class ETLMapper extends Mapper<LongWritable,Text,NullWritable,Text>
	{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			LogParser logparser = new LogParser();
			Map<String,String> map = logparser.ParseLog(value.toString());		
			
			String delim = "\t";

			Text output = new Text(map.get("ip")+delim+map.get("country")+delim+map.get("province")
			+delim+map.get("city")+delim+map.get("url")+delim+map.get("time")+
			delim+map.get("device")+delim+map.get("os")+delim+map.get("browser")+delim+map.get("referer"));
			
			
			context.write(NullWritable.get(), output);
 	 
		}
		
		
		
	}
	
	
	
	
	
	
	
	

}
