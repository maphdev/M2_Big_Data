package bigdata;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
       extends Mapper<Object, Text, Text, DoubleWritable>{
	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] tokens = line.split("\t");
		  if(tokens.length == 6){
			  String store = tokens[2];
			  double sale = Double.parseDouble(tokens[4]);
			  context.write(new Text(store), new DoubleWritable(sale));
		  }
	  }
  }
  public static class ProjectReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //context.write(key, value);
    	String oldStore = "";
    	double highestSale = 0;
    	for(DoubleWritable value: values){
    		String thisStore = key.toString();
    		double thisSale = Double.parseDouble(value.toString());
    		if(!oldStore.equals("") && !oldStore.equals(thisStore)){
    			context.write(new Text(oldStore), new DoubleWritable(highestSale));
        		highestSale = 0;
    		}
    		if(thisSale > highestSale){
    			highestSale = thisSale;
    		}
    		oldStore = thisStore;
    	}
    	if(!oldStore.equals("")){
			context.write(new Text(oldStore), new DoubleWritable(highestSale));
    	}
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Project");
    job.setNumReduceTasks(1);
    job.setJarByClass(Project.class);
    job.setMapperClass(ProjectMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setReducerClass(ProjectReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
