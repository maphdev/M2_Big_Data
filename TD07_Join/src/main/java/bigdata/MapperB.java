package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// region_codes.csv
public class MapperB extends Mapper<Object, Text, Text, TaggedValue>{
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] tokens = line.split(",");

		  if(!tokens[0].isEmpty() && !tokens[1].isEmpty() && !tokens[2].isEmpty()){
			  context.write(new Text(tokens[0].toLowerCase() + " " + tokens[1].toLowerCase()), new TaggedValue(TaggedValue.CATEGORY_REGION, line));
		  }
	  }
}