package bigdata;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<NullWritable, CityWritable, NullWritable,Text> {
	
	public int k = 0;

	private TreeMap<Integer, CityWritable> treeMap  = new TreeMap<Integer, CityWritable>();
	
	@Override
	public void setup(Context context) {
		  Configuration conf = context.getConfiguration();
		  k = conf.getInt("k", 10);
	}
	
	@Override
	public void reduce(NullWritable key, Iterable<CityWritable> values, Context context) throws IOException, InterruptedException {
		for (CityWritable c : values){
			
			treeMap.put(c.population, c.clone());
			if (treeMap.size() > k){
				treeMap.remove(treeMap.firstKey());
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Text("country, city, accent city, region, population, latitude, longitude"));
		for (CityWritable c : treeMap.descendingMap().values()){
			context.write(NullWritable.get(), new Text(c.toString()));
		}
	}
}