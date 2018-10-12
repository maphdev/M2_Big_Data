package bigdata;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
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
       extends Mapper<Object, Text, NullWritable, DoubleWritable>{
	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] tokens = line.split("\t");
		  if(tokens.length == 6){
			  double cost = Double.parseDouble(tokens[4]);
			  context.write(NullWritable.get(), new DoubleWritable(cost));
		  }
	  }
  }
  public static class ProjectReducer
       extends Reducer<NullWritable,DoubleWritable,NullWritable, Text> {
	  @Override
    public void reduce(NullWritable key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int nbSales = 0;
    	double totalSales = 0;
    	for(DoubleWritable value: values){
    		nbSales += 1;
    		double thisSale = Double.parseDouble(value.toString());
    		totalSales += thisSale;
	    }
    	
    	context.write(NullWritable.get(), new Text("Number of Sales\t"+String.valueOf(nbSales)));
    	context.write(NullWritable.get(), new Text("Total Value of Sales\t"+String.valueOf(totalSales)));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Project");
    job.setNumReduceTasks(1);
    job.setJarByClass(Project.class);
    job.setMapperClass(ProjectMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setReducerClass(ProjectReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
