package bigdata;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    // Parameter
    //conf.setInt("k", Integer.parseInt(args[2]));
    
    Job job1 = Job.getInstance(conf, "Project");
    job1.setNumReduceTasks(1);
    job1.setJarByClass(Project.class);
    
    // Mapper
    job1.setMapperClass(FilterMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    
    // Reducer
    job1.setReducerClass(FilterReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    
    // Format
    job1.setOutputFormatClass(TextOutputFormat.class);
    job1.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}
