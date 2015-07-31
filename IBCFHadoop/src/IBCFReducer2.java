import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Vishal Doshi
 *
 */

//Sample: [Matrix,Alien (5,1) Matrix,Alien (4,3) =>Matrix,Alien (-0.47)]
public class IBCFReducer2 extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		ArrayList<Integer> xVector = new ArrayList<Integer>();
		ArrayList<Integer> yVector = new ArrayList<Integer>();
		for(Text r: values){
			String ratings = r.toString();
			ratings=ratings.substring(1, ratings.length()-1);
			String[] ratingVals = ratings.split("\\,");
			xVector.add(Integer.parseInt(ratingVals[0]));
			yVector.add(Integer.parseInt(ratingVals[1]));
		}
		double sumOfProducts=0d;
		double sumOfX=0d;
		double sumOfY=0d;
		double sumOfSquareOfX=0d;
		double sumOfSquareOfY=0d;
		double similarity = -1d;
		if(xVector.size()!=1){
		for(int i=0;i<xVector.size();i++){
			int x=xVector.get(i);
			int y=yVector.get(i);
			sumOfProducts+=x*y;
			sumOfX+=x;
			sumOfY+=y;
			sumOfSquareOfX+=Math.pow(x,2);
			sumOfSquareOfY+=Math.pow(y,2);
		}
		
		//Calculate Pearson's Correlation Similarity
		try{
			double numerator = ((xVector.size()*sumOfProducts)-(sumOfX*sumOfY));
			double denominator = (Math.sqrt(Math.abs((((xVector.size()*sumOfSquareOfX)-(Math.pow(sumOfX, 2))) * ((xVector.size()*sumOfSquareOfY)-(Math.pow(sumOfY,2)))))));
			if(denominator != 0)
					similarity = numerator/denominator;
		}
		catch(ArithmeticException ae){
			System.out.println("Arithmatic Exception occured"+ae.toString());
		}
		similarity = Math.round(similarity * 1000) / 1000.0;
		}//End If
		context.write(key,new DoubleWritable(similarity));
		
	}
	
}
