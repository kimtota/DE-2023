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

public class IMDBStudent20180950
{
	public static class IMDBMapper extends Mapper<Object, Text, Text, Text> 
	{
		boolean fileR = true;

        	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        		Text _key = new Text();
			Text _value = new Text();
			
			String str = value.toString();
			String[] splitter = str.split("::");
			
			String title = "";

			
            		if (fileR) {
                		int cnt = 0;
			    	for (int i=0; i < splitter.length; i++) {
					if (cnt == 0) {
					}
				    	else if (cnt == 1) {
		                		_key.set(splitter[i]);
		            		}
		            		else if(cnt == 2) {
		                		_value.set("R|" + splitter[i]);
		                		context.write(_key, _value);
		            		}
		            		else if (cnt == 3) {
		            			cnt = -1;
		            		}
		            		cnt++;
		            	}
                	}
                	else {
				int cnt = 0;
				for (int i=0; i < splitter.length; i++) {
					if (cnt == 0) {
						_key.set(splitter[i]);
				    	}
				    	else if(cnt == 1) {
				    		title = splitter[i];
				    	}
				    	else if(cnt == 2) {
				        	StringTokenizer itr  = new StringTokenizer(splitter[i], "|");
				        	while (itr.hasMoreTokens()) {
				            		String g = itr.nextToken().trim();
				            		if (g.equals("Fantasy")) {
				            			_value.set("M|" + title);
				                		context.write( _key, _value);
				            		}
				        	}
				        	cnt = -1;
				    	}
				    	cnt++;
				}
			}
        	}
        	/**protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "movies.txt" ) != -1 ) fileM = true;
			else fileM = false;
		}**/
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "ratings.dat" ) != -1 ) fileR = true;
			else fileR = false;
		}
     	}

	public static class IMDBReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text reduce_key = new Text();
		 	Text reduce_result = new Text();
			
			String title = "";
            		double sum = 0;
            		double cnt = 0;
			
			for (Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), "|");
				String file_type = itr.nextToken().trim();

				if( file_type.equals("R") ) {
                    			int rating = Integer.parseInt(itr.nextToken().trim());
				    	sum += rating;
				    	cnt += 1;
				}
				else {
				    	title = itr.nextToken().trim();
				}
            		}
            		if (title.equals("")){
            		}
            		else {
            			sum = sum / cnt;
		    		String odf = String.format("%.2f", sum);
		    		reduce_key.set(title);
				reduce_result.set(odf);
				context.write(reduce_key, reduce_result);
            		}
		}
	}


	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        	
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: IMDB <in> <out> <topK>");
			System.exit(2);
		}

		Job job1 = new Job(conf, "imdb1");
		job1.setJarByClass(IMDBStudent20180950.class);
		job1.setMapperClass(IMDBMapper.class);
		//job1.setCombinerClass(IMDBReducer.class);
		job1.setReducerClass(IMDBReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		FileSystem.get(job1.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
