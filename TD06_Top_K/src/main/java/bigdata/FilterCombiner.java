package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterCombiner extends Reducer<Text,CityWritable,Text,CityWritable> {
	public void reduce(Text key, Iterable<CityWritable> values, Context context) throws IOException, InterruptedException {
	
		CityWritable maxPopCity = new CityWritable();
		for (CityWritable value: values){
			if (value.population > maxPopCity.population){
				maxPopCity = value;
			}
		}
		context.write(key, maxPopCity);
	}
}
