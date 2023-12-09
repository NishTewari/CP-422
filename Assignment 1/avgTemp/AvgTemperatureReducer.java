package avgTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		// Reduce Function Implementation
		int sum_Temperature = 0;
		int count = 0;

		for (IntWritable val : values) {
			sum_Temperature += val.get();
			count++;
		}
		// Calculate the average temperature
		int avg_Temperature = sum_Temperature / count;
		context.write(key, new IntWritable(avg_Temperature));
	}
}
