package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable {

	private long length;
	
	public FakeInputSplit() {}
	
	public FakeInputSplit(long nbPoints) {
		length = nbPoints;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(length);
	}

	public void readFields(DataInput in) throws IOException {
		length = in.readLong();
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

}
