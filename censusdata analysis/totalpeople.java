//total no.of people born in US
package census;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class totalpeople {
	public static class MapperClass extends
	  Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	 public void map(LongWritable key, Text censusdata, Context con)
	   throws IOException, InterruptedException {
	  String[] word = censusdata.toString().split(",");
	  	  String country=word[7];
	  	  try {
		  if(country.contentEquals(" United-States")){
			  
		   con.write(new Text("Total no of people born in US"),one);
	       }
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
			  for (IntWritable var : valueList) {
		 	var.get();	
		   sum++;	
					
				}
					//count++;
	   
	
	   result.set(sum);
		
		   con.write(key,result);
		  }
	   catch (Exception e) {
		   e.printStackTrace();
	
	   }
	 }
	}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "total people");
    job.setJarByClass(totalpeople.class);
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