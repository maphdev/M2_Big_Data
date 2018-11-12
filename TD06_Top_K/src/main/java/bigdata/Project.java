package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project {
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
	
	// Parameter
	conf2.setInt("k", Integer.parseInt(args[2]));

    Job job2 = Job.getInstance(conf2, "Project");
    job2.setNumReduceTasks(1);
    job2.setJarByClass(Project.class);
    
    // Mapper
    job2.setMapperClass(TopKMapper.class);
    job2.setMapOutputKeyClass(NullWritable.class);
    job2.setMapOutputValueClass(CityWritable.class);
    
    // Reducer
    job2.setReducerClass(TopKReducer.class);
    job2.setOutputKeyClass(NullWritable.class);
    job2.setOutputValueClass(CityWritable.class);
    
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
