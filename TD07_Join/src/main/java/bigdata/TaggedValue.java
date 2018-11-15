package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TaggedValue implements Writable{
	
	public static final Boolean CATEGORY_CITY = false;
	public static final Boolean CATEGORY_REGION = true;
	
	private Boolean category;
	private String content;
	
	public TaggedValue(){};
	
	public TaggedValue(Boolean category, String content) {
		this.category = category;
		this.content = content;
	}

	public Boolean getCategory() {
		return category;
	}

	public void setCategory(Boolean category) {
		this.category = category;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(category);
		out.writeUTF(content);
	}

	public void readFields(DataInput in) throws IOException {
		this.category = in.readBoolean();
		this.content = in.readUTF();
	}

}
