package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CityWritable implements Writable {
	public String country;
	public String city;
	public String accentCity;
	public String region;
	public int population = 0;
	public String latitude;
	public String longitude;
	
	public CityWritable(){}
	
	public CityWritable(String country, String city, String accentCity, String region, int population, String latitude, String longitude) {
		this.country = country;
		this.city = city;
		this.accentCity = accentCity;
		this.region = region;
		this.population = population;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(country);
		out.writeUTF(city);
		out.writeUTF(accentCity);
		out.writeUTF(region);
		out.writeInt(population);
		out.writeUTF(latitude);
		out.writeUTF(longitude);
	}

	public void readFields(DataInput in) throws IOException {
		this.country = in.readUTF();
		this.city = in.readUTF();
		this.accentCity = in.readUTF();
		this.region = in.readUTF();
		this.population = in.readInt();
		this.latitude = in.readUTF();
		this.longitude = in.readUTF();
	}
	
	public String toString(){
		StringBuilder strBd = new StringBuilder();
		strBd.append(country);
		strBd.append(", ");
		strBd.append(city);
		strBd.append(", ");
		strBd.append(accentCity);
		strBd.append(", ");
		strBd.append(region);
		strBd.append(", ");
		strBd.append(population);
		strBd.append(", ");
		strBd.append(latitude);
		strBd.append(", ");
		strBd.append(longitude);
		return strBd.toString();
	}
	
	public CityWritable clone(){
		return new CityWritable(country, city, accentCity, region, population, latitude, longitude);
	}
}
