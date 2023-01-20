import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import org.apache.hadoop.util.*;


public class FB {
	// Mapper class -> output -> string,int
	public static class FBMapper extends MapReduceBase implements 
	Mapper<Object, /*Input key type*/
	Text, /* input value type */
	Text, /* output key type*/
	IntWritable> /* output value type */
	{
		
		//Map fuction
		boolean flag =false;
		public void map(Object key, Text value,
		OutputCollector<Text, IntWritable> output,
		Reporter reporter) throws IOException
		{
		String line = value.toString();
		//String line = value.toString().split(",",10);
		if(flag) {
			StringTokenizer s = new StringTokenizer(line,",");
			String id = s.nextToken();
			String type = s.nextToken();
			String date = s.nextToken();
			// String type =line[1];
			// String date = line[2];
			// int likes = Integer.parseInt(line[6]);

		int count =0, likes = 0;
		while(count < 4) // it is use to skip column
		{
			likes = Integer.parseInt(s.nextToken());
			count++;
		}
		if(date.startsWith("2") && date.contains("2018") && type.equals("video"))
			output.collect(new Text("Likes"), new IntWritable(likes));
		}
		flag =true;
	}
}

	// Reducer class -> string,int
	public static class FBReducer extends MapReduceBase implements 
	Reducer<Text,
	IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,
			    Iterator<IntWritable> values,
			    OutputCollector<Text, IntWritable> output,
			    Reporter reporter) throws IOException
		 {
		 
		   int add = 0;
		   while (values.hasNext())
		   	add = add + values.next().get();
		   		
		   output.collect(key, new IntWritable(add));
		   	   
       }
     }

	public static void main(String args[]) throws Exception
	{
		JobConf conf = new JobConf(FB.class);
		conf.setJobName("Facebook Likes");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(FBMapper.class);
		conf.setReducerClass(FBReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}

