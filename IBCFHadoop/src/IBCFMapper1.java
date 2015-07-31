import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vishal Doshi
 *
 */

//Sample: [(Alice,Matrix,5) (Alice, Alien, 1)=> Alice (Matrix,5) Alice (Alien,1)]

public class IBCFMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String[] columns = val.split("\t");
		
		context.write(new Text(columns[0]), new Text("("+columns[1]+","+columns[2]+")"));
	}
}
