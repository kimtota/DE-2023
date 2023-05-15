import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20180950
{

	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String str = value.toString();
			String[] splitter = str.split("::");
			
			int cnt = 0;
			for (int i=0; i < splitter.length; i++) {
				if(cnt == 2) {
					StringTokenizer itr  = new StringTokenizer(splitter[i], "|");
					while (itr.hasMoreTokens()) {
						word.set(itr.nextToken());
						context.write(word, one);
					}
					cnt = 0;
				}
				cnt++;
			}
			/**StringTokenizer itr = new StringTokenizer(value.toString(), "'::'");
			int cnt = 0;
			while (itr.hasMoreTokens())
			{
				String str = itr.nextToken().trim();
				cnt++;
				
				if(cnt == 3) {
					StringTokenizer itr2  = new StringTokenizer(str, "|");
					while (itr2.hasMoreTokens()) {
						word.set(itr2.nextToken());
						context.write(word, one);
					}
					cnt = 0;
				}
			}**/
			
		}
	}

	public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(IMDBStudent20180950.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
