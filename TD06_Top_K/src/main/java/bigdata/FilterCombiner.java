package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		int max = -1;
		for (IntWritable value: values){
			int population = value.get();
			if (population > max){
				max = population;
			}
		}
		context.write(key, new IntWritable(max));
	}
}
