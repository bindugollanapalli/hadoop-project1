//total no of widowed working females
package census;
import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
///import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class totalfemales {
	public static class MapperClass extends
	  Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	 public void map(LongWritable key, Text censusdata, Context con)
	   throws IOException, InterruptedException {
	  String[] word = censusdata.toString().split(",");
	  int weeksworked = Integer.parseInt(word[9]);
	  String gender=word[3];
	  String maritalstatus=word[2];
	  try {
		  if(maritalstatus.contentEquals(" Widowed") && weeksworked>0 && gender.contentEquals(" Female") ){
			  
		   con.write(new Text("Widowed_females_working"),one);
	       }
//		  else
//		  {
//			  con.write(new Text("Widowedfemales"), new IntWritable(8989)); 
//		  }
		  }
	  catch (Exception e) {
		   e.printStackTrace();
		  }
	 }
	}
	public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> valueList,Context con) throws IOException, InterruptedException {
		  try {
	  
			  int sum=0;
			  //int count = 0;
	   for (IntWritable var : valueList) {
		 	var.get();	
		   sum++;	
					
				}
					//count++;
	   
	  // String str = String.format("%d", sum);
	   result.set(sum);
		  //String out = "count: " + count;
		   con.write(key,result);
		  }
	   catch (Exception e) {
		   e.printStackTrace();
	
	   }
	 }
	}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "total no.of widowed females");
    job.setJarByClass(totalfemales.class);
    job.setMapperClass(MapperClass.class);
   // job.setCombinerClass(IntSumReducer.class);
    //job.setNumReduceTasks(0);
    job.setReducerClass(ReducerClass.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}