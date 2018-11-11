package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointInputFormat extends InputFormat<LongWritable, Point2DWritable>{

	private static int nbSplits = 1;
	private static int nbPointsPerSplit = 1;
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>(nbSplits);
		for(int i = 0; i < nbSplits; ++i){
			splits.add(new FakeInputSplit(nbPointsPerSplit));
		}
		return splits;
	}

	@Override
	public RecordReader<LongWritable, Point2DWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new RandomPointReader();
	}

	public static void setNbSplits(int nb) {
		if (nb > 0) nbSplits = nb;
	}

	public static void setNbPointsPerSplit(int nb) {
		if (nb > 0) nbPointsPerSplit = nb;
	}	
}
