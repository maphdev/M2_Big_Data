package bigdata;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper<NullWritable, CityWritable, NullWritable, CityWritable>{
	
	public int k = 0;

	private TreeSet<CityWritable> treeSet  = new TreeSet<CityWritable>();

	@Override
	public void setup(Context context) {
		  Configuration conf = context.getConfiguration();
		  k = conf.getInt("k", 10);
	}
	
	@Override
	public void map(NullWritable key, CityWritable value, Context context) throws IOException, InterruptedException {
		treeSet.add(value.clone());
		
		if(treeSet.size() > k){
			treeSet.remove(treeSet.first());
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (CityWritable c : treeSet.descendingSet()){
			context.write(NullWritable.get(), c);
		}
	}
}