package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point2DWritable {
	public double x;
	public double y;
	  
	public Point2DWritable(){}
	
	public Point2DWritable(double x, double y){
		this.x = x;
		this.y = y;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}

	public void readFields(DataInput in) throws IOException {
		in.readDouble();
		in.readDouble();
	}
}
