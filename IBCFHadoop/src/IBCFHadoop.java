import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 */

/**
 * @author Vishal Doshi
 *
 */

//Arguments List: ibcfInput ibcfOutput1 ibcfOutput2 ibcfOutput3
public class IBCFHadoop extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//Exit if the paths not provided.
		if(args.length!=4){
			System.out.println("Invalid Parameters");
			return -1;
		}
							
			        //Creating a Job 1
		Configuration conf1 = getConf(); 

		        
		String ip,op;
		Job job1 = new Job(conf1, "IBCF Phase 1");
			      
				    //Driver Class
		job1.setJarByClass(IBCFHadoop.class);     
				    //Setting Mapper, Reducer and Partitioner Classes
		job1.setMapperClass(IBCFMapper1.class);
		job1.setReducerClass(IBCFReducer1.class);
		  
			        //Setting Mapper Key and Value output type
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
			        //Setting final Key and Value Type
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
			        			        
			        //Set number of Reducers for job1
		job1.setNumReduceTasks(4);
			
			        //Creating file-paths for job1
		ip=args[0];	
		op = args[1];	
			        
			        //Setting the paths for job1
		FileInputFormat.setInputPaths(job1, new Path(ip));
		FileOutputFormat.setOutputPath(job1, new Path(op));
			        
			        //Running the job1.
		job1.waitForCompletion(true);
			        
// =====================================JOB 2======================================================
		Configuration conf2 = getConf(); 

		conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
			        
		Job job2 = new Job(conf2, "IBCF Phase 2");
				      
			        //Driver Class
		job2.setJarByClass(IBCFHadoop.class);
			        
			        //Setting Mapper, Reducer and Partitioner Classes
		job2.setMapperClass(IBCFMapper2.class);
		job2.setReducerClass(IBCFReducer2.class);
		  
			        //Setting Mapper Key and Value output type
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
			        //Setting final Key and Value Type
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
			   
					//Setting the key-value to text input format
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
			        
			        //Set number of Reducers
		job2.setNumReduceTasks(4);
			
			        //Creating file-paths
		ip=args[1];	
		op = args[2];
			        
			        //Setting the paths
		FileInputFormat.setInputPaths(job2, new Path(ip));
		FileOutputFormat.setOutputPath(job2, new Path(op));
			        
			        //Running the job.
		job2.waitForCompletion(true);
			        
// =====================================JOB 3=========================================
		Configuration conf3 = getConf(); // THIS IS THE CORRECTWAY

		conf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
			        
		Job job3 = new Job(conf3, "IBCF Phase 3: Sorting as per Correaltion value");
				      
			        //Driver Class
		job3.setJarByClass(IBCFHadoop.class);
			        
			        //Setting Mapper, Reducer and Partitioner Classes
		job3.setMapperClass(IBCFMapper3.class);
		  
			        //Setting Mapper Key and Value output type
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
			        
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
			        

			
			        //Creating file-paths
		ip=args[2];	
		op = args[3];
			        
			        //Setting the paths
		FileInputFormat.setInputPaths(job3, new Path(ip));
		FileOutputFormat.setOutputPath(job3, new Path(op));
			        
			        //Running the job.
		job3.waitForCompletion(true);
		
//			         Getting top 100
		getTop100(args);
		return 0;
	}
	
	public void getTop100(String args[]) throws Exception{
		// Getting top 100
		//Sample: Alien, Inception 0.47
        
        // Saving movie names in ArrayList
        Path pt=new Path("movieDetails/u.item");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br1=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        ArrayList<String> al = new ArrayList<String>();
        line=br1.readLine();
        while (line != null){
                String[] movieDetails=line.split("\\|");
                al.add(movieDetails[1]);
                line=br1.readLine();	        
        }
        
        // Reading the result of Job 3
        Path pt2 = new Path(args[3]+"/part-r-00000");
        BufferedReader br2 = new BufferedReader(new InputStreamReader(fs.open(pt2)));
        
        // Creating a new file with top 100 movie pairs and values
        Path pt3=new Path("movieDetails/top100.txt");
        BufferedWriter bw3=new BufferedWriter(new OutputStreamWriter(fs.create(pt3,true)));
        String lineR;
        String lineW; 
        for(int i = 0; i<100; i++){
        	  lineR=br2.readLine();
        	  lineW="";
        	  String[] lr = lineR.split("\t");
        	  String[] movieIds = lr[1].split(",");
        	  // Get movie names
        	  String x1 = al.get(Integer.parseInt(movieIds[0])-1);
        	  String y1 = al.get(Integer.parseInt(movieIds[1])-1);
        	  
        	  //Change the Polarity, so the top 100 has the actual values calculated.
        	  double simi = Double.parseDouble(lr[0]) * -1.0;
        	  lineW=x1+","+y1+"\t"+simi+"\n";
        	  //Write to the file
        	  bw3.write(lineW);
        }
        br1.close();
        br2.close();
        bw3.close();
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new IBCFHadoop(), args);
        System.exit(res);
		

	}

}
