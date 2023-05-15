import java.io.IOException;
import java.util.*;
import java.time.*;
import java.time.format.TextStyle;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20180950
{
	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		private Text region_day = new Text();
		private Text trips_vehicles = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String region = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			int vehicles = Integer.parseInt(itr.nextToken().trim());
			int trips = Integer.parseInt(itr.nextToken().trim());
			String n_day = null;
			
			StringTokenizer itr2 = new StringTokenizer(date, "/");
			int month = Integer.parseInt(itr2.nextToken().trim());
			int day = Integer.parseInt(itr2.nextToken().trim());
			int year = Integer.parseInt(itr2.nextToken().trim());
			LocalDate processedDate = LocalDate.of(year, month, day);
			DayOfWeek dayOfWeek = processedDate.getDayOfWeek();
			n_day = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US);
			n_day = n_day.toUpperCase();
			if (n_day.equals("THU")) n_day = "THR";
			
			region_day.set(region + "," + n_day);
			trips_vehicles.set(trips + "," + vehicles);
			context.write(region_day, trips_vehicles);
		}
	}

	public static class UBERReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sumT = 0;
			int sumV = 0;
			for(Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				int t = Integer.parseInt(itr.nextToken().trim());
				int v = Integer.parseInt(itr.nextToken().trim());
				sumT += t;
				sumV += v;
			}
			result.set(sumT + "," + sumV);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: uber <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "uber");
		
		job.setJarByClass(UBERStudent20180950.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
