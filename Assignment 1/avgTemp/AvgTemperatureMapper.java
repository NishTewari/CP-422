package avgTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Mapper Implementation

		String line = value.toString();
		String year = line.substring(15, 19);
		String month = line.substring(19, 21);
		String time = line.substring(23, 27);

		// Filter the temperature values by 1pm
		if (time.equals("1300")) {

			int airTemperature;
			if (line.charAt(87) == '+') {
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}

			String quality = line.substring(92, 93);
			// Filter missing values & validate the recode
			if (airTemperature != MISSING && quality.matches("[01459]")) {
				context.write(new Text(year + month), new IntWritable(airTemperature));
			}
		}

	}
}
