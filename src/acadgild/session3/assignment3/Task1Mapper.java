package acadgild.session3.assignment3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author acadgild
 *
 */
public class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String[] array = value.toString().split("[|]");
		
		String InvalidColumnVal="NA";

		Text company = new Text(array[0]);
		Text product = new Text(array[1]);

		IntWritable noOfItems = new IntWritable(1);

		if (!(InvalidColumnVal.equalsIgnoreCase(company.toString())) && !(InvalidColumnVal.equalsIgnoreCase(product.toString()))) {
			context.write(company, noOfItems);
		}
	}

}