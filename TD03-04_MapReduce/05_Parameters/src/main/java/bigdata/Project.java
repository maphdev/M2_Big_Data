package bigdata;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project {
	public static class ProjectMapper extends Mapper<Object, Text, IntWritable, StatWritable>{
		
		private double discretization;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {			
			discretization = (double) context.getConfiguration().getInt("param", 1);
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			if(!tokens[4].equals("") && !tokens[4].equals("Population")){
				String population = tokens[4];
				double log = floorToValue(Math.log10(Double.parseDouble(population)), discretization);
				int eq_class = (int) Math.pow((double) 10, (double) log);
				context.write(new IntWritable(eq_class), new StatWritable(Integer.parseInt(population), 1, 0, 0));
			}
		}
		
		public static double floorToValue(double d, double param){
			return Math.floor(d * param) / param;
		}
	}
  
	public static class ProjectCombiner extends Reducer<IntWritable,StatWritable,IntWritable,StatWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<StatWritable> values, Context context) throws IOException, InterruptedException {
			int partialSum = 0;
			int partialCount = 0;
			int tempMin = -1;
			int tempMax = -1;
			for (StatWritable value: values){
				partialCount += value.count;
				int population = value.sum;
				partialSum += population;
				if (tempMin == -1 || tempMax == -1){
					tempMin = population;
					tempMax = population;
				}
				if(population < tempMin){
					tempMin = population;
	    		}
	    		if(population > tempMax){
	    			tempMax = population;
	    		}
			}
			context.write(key, new StatWritable(partialSum, partialCount, tempMin, tempMax));
		}
	}
	
	public static class ProjectReducer extends Reducer<IntWritable,StatWritable,IntWritable,Text> {
		@Override
		public void reduce(IntWritable key, Iterable<StatWritable> values, Context context) throws IOException, InterruptedException {
			double totalSum = 0;
			double totalCount = 0;
			int min = -1;
			int max = -1;

			for(StatWritable value: values) {
				totalCount += value.count;
				totalSum += value.sum;
				if (min == -1 || max == -1){
					min = value.min;
					max = value.max;
				}
				if(value.min < min){
					min = value.min;
	    		}
	    		if(value.max > max){
	    			max = value.max;
	    		}
			}

			String summary = (int) totalCount + "\t" + totalSum/totalCount + "\t" + max + "\t" + min;
			
			context.write(key, new Text(summary));
		}
	}
	
	public static class StatWritable implements Writable {
		public int sum;
		public int count;
		public int min;
		public int max;
		
		public StatWritable(){}
		
		public StatWritable(int sum, int count, int min, int max){
			this.sum = sum;
			this.count = count;
			this.min= min;
			this.max = max;
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeInt(sum);
			out.writeInt(count);
			out.writeInt(min);
			out.writeInt(max);			
		}

		public void readFields(DataInput in) throws IOException {
			sum = in.readInt();
			count = in.readInt();
			min = in.readInt();
			max = in.readInt();			
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    
		// Parameter
		conf.setInt("param", Integer.parseInt(args[2]));
		
		Job job = Job.getInstance(conf, "Project");
		job.setNumReduceTasks(1);
		job.setJarByClass(Project.class);
		
		// mapper
		job.setMapperClass(ProjectMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(StatWritable.class);
		
		// combiner
		job.setCombinerClass(ProjectCombiner.class);
		
		// reducer
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
