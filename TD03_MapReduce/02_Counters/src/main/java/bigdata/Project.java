package bigdata;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/* 
** Resources about counters
** https://acadgild.com/blog/counters-in-mapreduce
** https://sharebigdata.wordpress.com/2015/11/30/counters-in-map-reduer/
*/
 
public class Project {
	
	// WCP is a group of 3 counters : nb_cities, nb_pop, total_pop
	public static enum WCP {
		nb_cities,
		nb_pop,
		total_pop
	};
	
	public static class ProjectMapper extends Mapper<Object, Text, Text, NullWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			// number of cities
			context.getCounter(WCP.nb_cities).increment(1);
			if(!tokens[4].equals("") && !tokens[4].equals("Population")){
				// number of cities with known population
				context.getCounter(WCP.nb_pop).increment(1);
				// number of inhabitants in all cities with known population
				context.getCounter(WCP.total_pop).increment(Long.parseLong(tokens[4]));
				// send <key, value> to reducer
				context.write(value, NullWritable.get());
			}
		}
	}
  
	public static class ProjectReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
  
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Project");
		job.setNumReduceTasks(1);
		job.setJarByClass(Project.class);
		job.setMapperClass(ProjectMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(ProjectReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// counters
		Counters counters = job.getCounters();
		Counter c_nb_cities = counters.findCounter(WCP.nb_cities);
		System.out.println(c_nb_cities.getDisplayName()+" : "+c_nb_cities.getValue());
		Counter c_nb_pop = counters.findCounter(WCP.nb_pop);
		System.out.println(c_nb_pop.getDisplayName()+" : "+c_nb_pop.getValue());
		Counter c_total_pop = counters.findCounter(WCP.total_pop);
		System.out.println(c_total_pop.getDisplayName()+" : "+c_total_pop.getValue());
	}
}
