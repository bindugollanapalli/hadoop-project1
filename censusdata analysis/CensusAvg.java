//average age of divorced people 
package census;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CensusAvg {
		public static class MapperClass extends
		  Mapper<LongWritable, Text, Text, LongWritable> {
		 public void map(LongWritable key, Text censusdata, Context con)
		   throws IOException, InterruptedException {
		  String[] word = censusdata.toString().split(",");
		 String maritalstatus = word[2];
		  try {
			  if(maritalstatus.contentEquals(" Divorced")){
		   Long age = Long.parseLong(word[0]);
		   con.write(new Text("averge age"), new LongWritable(age));
		  } 
		  }
		  catch (Exception e) {
		   e.printStackTrace();
		  }
		 }
		}

		public static class ReducerClass extends
		  Reducer<Text, LongWritable, Text,LongWritable> {
		 public void reduce(Text key, Iterable<LongWritable> valueList,
		   Context con) throws IOException, InterruptedException {
		  try {
		   long total=0;
		   long count=0;
		   for (LongWritable var : valueList) {
		    total+= var.get();
		    ///System.out.println("reducer " + var.get());
		    count+=1;
		   }
		   Long avg = total / count;
		  // String out = "Total: " + total + " :: " + "Average: " + avg;
		   con.write(key, new LongWritable(avg));
		  } 
		  catch (Exception e) {
		   e.printStackTrace();
		  }
		 }
		}
		public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "census avg");
		    job.setJarByClass(CensusAvg.class);
		    job.setMapperClass(MapperClass.class);
		   // job.setCombinerClass(IntSumReducer.class);
		   // job.setNumReduceTasks(0);
		    job.setReducerClass(ReducerClass.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
		}
