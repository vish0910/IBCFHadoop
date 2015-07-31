import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Vishal Doshi
 *
 */
//Sample: [(Alien, Inception)   -0.47 => 0.47   (Alien,Inception)

public class IBCFMapper3 extends Mapper<Text, Text, DoubleWritable, Text> {
	
	@Override
	public void map(Text key,Text value, Context context) throws IOException, InterruptedException{
		double correlation = Double.parseDouble(value.toString());
		//Change the Polarity, so you get reverse sorted list.
		correlation=correlation*(-1.0);
		context.write(new DoubleWritable(correlation),key);
	}

}
