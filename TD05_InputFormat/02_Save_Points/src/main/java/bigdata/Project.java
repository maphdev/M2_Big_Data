package bigdata;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project {
	
  public static enum COUNT{
	  Pi
  };
	
  public static class ProjectMapper extends Mapper<LongWritable, Point2DWritable, NullWritable, NullWritable>{
	  public void map(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
		  if (value.x * value.x + value.y * value.y < 1){
			  context.getCounter("Pi", "in").increment(1);;
		  } else {
			  context.getCounter("Pi", "out").increment(1);
		  }
	  }
  }
  
  public static void main(String[] args) throws Exception {
	  
	int nbSplits = Integer.parseInt(args[0]);
	int nbPointsPerSplit = Integer.parseInt(args[1]);
	
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "InputFormat compute PI");
    job.setNumReduceTasks(0);
    job.setJarByClass(Project.class);
    RandomPointInputFormat.setNbSplits(nbSplits);
    RandomPointInputFormat.setNbPointsPerSplit(nbPointsPerSplit);
    job.setInputFormatClass(RandomPointInputFormat.class);
    job.setMapperClass(ProjectMapper.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.waitForCompletion(true);
    
    long inc = job.getCounters().findCounter("Pi", "in").getValue();
    long outc = job.getCounters().findCounter("Pi", "out").getValue();
    
    System.out.println("Pi = " + (double)inc * 4. / (double)(outc+inc));
  }
}
