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
		
		double discretization = 1.0;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			String param = context.getConfiguration().get("param");
			discretization = Double.parseDouble(param);
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			if(!tokens[4].equals("") && !tokens[4].equals("Population")){
				String population = tokens[4];
				double log = floorToValue(Math.log10(Double.parseDouble(population)), discretization);
				int eq_class = (int) Math.pow((double) 10, (double) log);
				context.write(new IntWritable(eq_class), new IntWritable(Integer.parseInt(population)));
			}
		}
		
		public static double floorToValue(double d, double param){
			return Math.floor(d * param) / param;
		}
	}
  
	public static class ProjectReducer extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int nbCities = 0;
			int sumPop = 0;
			int min = -1;
			int max = -1;
	    	for(IntWritable value: values){
	    		nbCities += 1;
	    		int population = value.get();
	    		sumPop += population;
	    		if(min == -1 || max == -1){
	    			min = population;
	    			max = population;
	    		}
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
	    
		// Parameter
		conf.set("param", args[2]);
		
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