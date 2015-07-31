import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Vishal Doshi
 *
 */

//Sample: [Alice (Matrix,5) Alice (Alien,1) => Alice (Matrix,5) (Alien, 1)] 
public class IBCFReducer1 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException{
		String movieRating="";
		for(Text val: values){
			movieRating+= val;
		}
		context.write(key, new Text(movieRating));
	}	
}
