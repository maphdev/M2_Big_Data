package bigdata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project {
  public static void main(String[] args) throws Exception {
	
	// configuration
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MultipleInputExample");
    job.setJarByClass(Project.class);
    
	// MultipleInputs
    MultipleInputs.addInputPath(
    		job,
    		new Path(args[0]),
    		TextInputFormat.class,
    		MapperA.class);
    MultipleInputs.addInputPath(job,
    		new Path(args[1]),
    		TextInputFormat.class,
    		MapperB.class);
    
    // Mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TaggedValue.class);

    // Reducer
    job.setNumReduceTasks(1);
    job.setReducerClass(ReducerJoin.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
    
	// output
    job.setOutputFormatClass(TextOutputFormat.class);
	TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
