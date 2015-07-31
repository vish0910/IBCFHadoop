import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vishal Doshi
 *
 */

//Sample:[Alice (Matrix,5)(Alien,1)(Inception,4) =>Matrix,Alien (5,1) Matrix,Inception (5,4) Alien,Inception (1,4)]

public class IBCFMapper2 extends Mapper<Text, Text, Text, Text> {
	@Override
	public void map(Text key, Text values, Context context) throws IOException, InterruptedException{
		ArrayList<MovieRating> ls = new ArrayList<MovieRating>();
		String val = values.toString();
		String[] str= val.split("\\)");
		for(String s : str){
			MovieRating mr= new MovieRating(s);
			ls.add(mr);
		}
		Collections.sort(ls);
		int sizeOfList = ls.size() - 1;
		for(int i= 0;i<sizeOfList;i++){
			for(int j=i+1;j<=sizeOfList;j++){
				String mapperOutKey=ls.get(i).movieId+","+ls.get(j).movieId;
				String mapperOutValue="("+ls.get(i).movieRating+","+ls.get(j).movieRating+")";
				context.write(new Text(mapperOutKey), new Text(mapperOutValue));
			}
		}
	}
}
