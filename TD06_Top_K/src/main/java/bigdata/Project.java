package bigdata;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project {
	// Map-reduce first job : filter
	public static class FilterMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			if(StringUtils.isNumeric(tokens[4]) && !tokens[4].equals("") && !tokens[4].equals("Population") && !tokens[2].equals("")){
				context.write(new Text(tokens[2]), new IntWritable(Integer.parseInt(tokens[4])));
			}
		}
	}
	
	public static class FilterCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
			int max = -1;
			for (IntWritable value: values){
				int population = value.get();
				if (population > max){
					max = population;
				}
			}
			context.write(key, new IntWritable(max));
		}
	}
	
	public static class FilterReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
			int max = -1;
			for (IntWritable value: values){
				int population = value.get();
				if (population > max){
					max = population;
				}
			}
			context.write(key, new IntWritable(max));
		}
	}
  
	// Map-reduce second job : Top K
	public static class TopKMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
		@Override
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			// some code
			context.write(key, value);
		}
	}
	
	public static class TopKReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (IntWritable value: values){
				context.write(key, value);
			}
		}
	}

  public static void main(String[] args) throws Exception { 
    // Job 1 : filter
	Configuration conf1 = new Configuration();

   
    Job job1 = Job.getInstance(conf1, "Project");
    job1.setNumReduceTasks(1);
    job1.setJarByClass(Project.class);
    
    // Mapper
    job1.setMapperClass(FilterMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    
    // Combiner
    job1.setCombinerClass(FilterCombiner.class);
    
    // Reducer
    job1.setReducerClass(FilterReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    
    // Format
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("first_job_output"));
    
    job1.waitForCompletion(true);
    
    // Job 2 : Top K
    
	Configuration conf2 = new Configuration();

    Job job2 = Job.getInstance(conf2, "Project");
    job2.setNumReduceTasks(1);
    job2.setJarByClass(Project.class);
    
    // Mapper
    job2.setMapperClass(TopKMapper.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
    
    // Reducer
    job2.setReducerClass(TopKReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    
    // Format
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    
    FileInputFormat.addInputPath(job2, new Path("first_job_output/part-r-00000"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    
    int returnValue = job2.waitForCompletion(true) ? 0 : 1;
    
    // delete temp file
    FileSystem.get(conf2).delete(new Path("first_job_output"), true);
    
    System.exit(returnValue);
  }
}
