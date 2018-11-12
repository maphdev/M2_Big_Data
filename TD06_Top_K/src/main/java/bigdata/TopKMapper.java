package bigdata;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper<NullWritable, CityWritable, NullWritable, CityWritable>{
	
	public int k = 0;

	private TreeMap<Integer, CityWritable> treeMap  = new TreeMap<Integer, CityWritable>();

	@Override
	public void setup(Context context) {
		  Configuration conf = context.getConfiguration();
		  k = conf.getInt("k", 10);
	}
	
	@Override
	public void map(NullWritable key, CityWritable value, Context context) throws IOException, InterruptedException {
		treeMap.put(value.population, value.clone());
		
		if(treeMap.size() > k){
			treeMap.remove(treeMap.firstKey());
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (CityWritable c : treeMap.values()){
			context.write(NullWritable.get(), c);
		}
	}
}