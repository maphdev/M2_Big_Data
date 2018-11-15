package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

// worldcitiespop.txt
public class MapperA extends Mapper<Object, Text, Text, TaggedValue>{
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  if ( ((LongWritable) key).get() == 0 ) return;
		  
		  String line = value.toString();
		  String[] tokens = line.split(",");
		  
		  if(!tokens[0].isEmpty() && !tokens[1].isEmpty() && !tokens[3].isEmpty()){
			  context.write(new Text(tokens[0].toLowerCase() + " " + tokens[3].toLowerCase()), new TaggedValue(TaggedValue.CATEGORY_CITY, line));
		  }
	  }
}
