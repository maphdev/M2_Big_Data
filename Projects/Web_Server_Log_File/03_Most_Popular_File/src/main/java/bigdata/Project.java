package bigdata;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project {
  public static class ProjectMapper
       extends Mapper<Object, Text, Text, IntWritable>{
	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
		  String[] tokens = value.toString().split(" ");
		  String path = tokens[6];
		  String[] splitPath = path.split("http://www.the-associates.co.uk");
		  if (splitPath.length > 1)
			  path = splitPath[1];
		  context.write(new Text(path), new IntWritable(1));
	  }
  }
  public static class ProjectReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	  private Text popular_file_path = new Text();
	  private IntWritable nbHit = new IntWritable(0);
	  
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	  int thisSum = 0;
    	  for(IntWritable value: values){
    		  thisSum += value.get();
    	  }
    	  if(thisSum > nbHit.get()){
    		  nbHit.set(thisSum);
    		  popular_file_path.set(key);
    	  }
    	  
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(popular_file_path, nbHit);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Project");
    job.setNumReduceTasks(1);
    job.setJarByClass(Project.class);
    job.setMapperClass(ProjectMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(ProjectReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
