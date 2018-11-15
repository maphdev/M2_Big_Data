package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoin extends Reducer<Text,TaggedValue,Text,Text> {
	
	private ArrayList<Text> listCity = new ArrayList<Text>();
	private ArrayList<Text> listRegion = new ArrayList<Text>();
	
    public void reduce(Text key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
    	listCity.clear();
    	listRegion.clear();
    	
    	for (TaggedValue value : values){
    		if (value.getCategory() == TaggedValue.CATEGORY_CITY){
    			listCity.add(new Text(value.getContent()));
    		} else if (value.getCategory() == TaggedValue.CATEGORY_REGION){
    			listRegion.add(new Text(value.getContent()));
    		}
    	}
    	
    	executeJoin(context);
    }
    
    private void executeJoin(Context context) throws IOException, InterruptedException {
    	if (!listCity.isEmpty() && !listRegion.isEmpty()) {
    		for (Text city: listCity){
    			for (Text region : listRegion){
    				context.write(city, region);
    			}
    		}
    	}
    }
  }