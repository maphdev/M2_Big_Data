package bigdata;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, CityWritable>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");
		if(StringUtils.isNumeric(tokens[4]) && !tokens[4].equals("") && !tokens[4].equals("Population") && !tokens[2].equals("")){
			context.write(new Text(tokens[2]), new CityWritable(tokens[0], tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4]), tokens[5], tokens[6]));
		}
	}
}
