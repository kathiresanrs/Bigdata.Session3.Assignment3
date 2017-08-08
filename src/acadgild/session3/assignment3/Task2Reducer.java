package acadgild.session3.assignment3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author acadgild
 *
 */
public class Task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		IntWritable retVal = null;
		int total = 0;

		// Return if the key is invalid (value is NA)
		if ("NA".equalsIgnoreCase(key.toString().trim())) {
			return;
		}

		for (IntWritable value : values) {
			total = total + value.get();
		}

		retVal = new IntWritable(total);
		context.write(key, retVal);
	}

}