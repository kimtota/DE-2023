import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin 
{
	public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		boolean fileA = true;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			Text outputKey = new Text();
			Text outputValue = new Text();
			String joinKey = "";
			String o_value = "";

			if( fileA ) {
				// join_key를 추출, 나머지 레코드와 문자A를 붙여서 emit
				o_value = "A" + ",";
				o_value += itr.nextToken().trim() + ",";
				o_value += itr.nextToken().trim();
				joinKey = itr.nextToken().trim();
			}
			else {
				// join_key를 추출, 나머지 레코드와 문자B를 붙여서 emit
				o_value = "B" + ",";
				o_value += itr.nextToken().trim() + ",";
				o_value += itr.nextToken().trim();
				joinKey = itr.nextToken().trim();
			}
			outputKey.set( joinKey );
			outputValue.set( o_value );
			context.write( outputKey, outputValue );
		}

		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "relation_a" ) != -1 ) fileA = true;
			else fileA = false;
		}
	}
	
	public static class ReduceSideJoinReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			String description = ""; // table B's info
			ArrayList<String> buffer = new ArrayList<String>(); // 초기화
			
			for (Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				String file_type = itr.nextToken().trim();

				if( file_type.equals( "B" ) ) {
					description = itr.nextToken().trim();
				}
				else {
					if ( description.length() == 0 ) {
						buffer.add( val.toString() );
					}
					else {
						String id = itr.nextToken().trim();
						String price = itr.nextToken().trim();
						reduce_key.set(id);
						reduce_result.set(price + " " + description);
						context.write(reduce_key, reduce_result);
					}
				}
			}

			for ( int i = 0 ; i < buffer.size(); i++ ) {
				Text val = buffer.get(i); 뭔가 추가?
				
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				String file_type = itr.nextToken().trim();
				String id = itr.nextToken().trim();
				String price = itr.nextToken().trim();

				reduce_key.set(id);
				reduce_result.set(price + " " + description);
				context.write(reduce_key, reduce_result);
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = 3;
		if (otherArgs.length != 2) {
			System.err.println("Usage: ReduceSideJoin <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", topK);

		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
