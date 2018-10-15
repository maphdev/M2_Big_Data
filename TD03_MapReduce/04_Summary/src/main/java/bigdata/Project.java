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
	public static class ProjectMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			if(!tokens[4].equals("") && !tokens[4].equals("Population")){
				String population = tokens[4];
				int log = (int) Math.log10(Double.parseDouble(population));
				int eq_class = (int) Math.pow((double) 10, (double) log);
				context.write(new IntWritable(eq_class), new IntWritable(Integer.parseInt(population)));
			}
		}
	}
  
	public static class ProjectReducer extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
				
		/*@Override
		public void setup(Context context) throws IOException, InterruptedException {
		}*/
		
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int nbCities = 0;
			int sumPop = 0;
			int min = key.get()*10-1;
			int max = key.get();
	    	for(IntWritable value: values){
	    		nbCities += 1;
	    		int population = value.get();
	    		sumPop += population;
	    		if(population < min){
	    			min = population;
	    		}
	    		if(population > max){
	    			max = population;
	    		}
	    	}
	    	double avg = (double)sumPop/(double)nbCities;
	    	
	    	String summary = nbCities + "\t" + avg + "\t" + max + "\t" + min;
	    	context.write(key, new Text(summary));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Project");
		job.setNumReduceTasks(1);
		job.setJarByClass(Project.class);
		job.setMapperClass(ProjectMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ProjectReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
