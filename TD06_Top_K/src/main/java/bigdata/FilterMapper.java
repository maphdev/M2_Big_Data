package bigdata;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, CityWritable>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if ( ((LongWritable) key).get() == 0 ) return;
		
		String[] tokens = value.toString().split(",");
		
		if(StringUtils.isNumeric(tokens[4]) && !tokens[4].isEmpty() && !tokens[4].equals("Population") && !tokens[1].isEmpty() && !tokens[0].isEmpty()){
			context.write(new Text(tokens[0] + ", " + tokens[1]), new CityWritable(tokens[0], tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4]), tokens[5], tokens[6]));
		}
	}
}
