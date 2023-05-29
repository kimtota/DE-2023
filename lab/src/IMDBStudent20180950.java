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
	public static class IMDBMapper extends Mapper<Object, Text, Text, Text> {
		boolean fileM = true;
		private Text _key = new Text();
		private Text _value = new Text();

        	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			String[] splitter = str.split("::");

			if( fileM ) {
				int cnt = 0;
				for (int i=0; i < splitter.length; i++) {
					if (cnt == 0) {
						_key.set(splitter[i]);
				    	}
				    	else if(cnt == 2) {
				        	StringTokenizer itr  = new StringTokenizer(splitter[i], "|");
				        	while (itr.hasMoreTokens()) {
				            		String g = itr.nextToken().trim();
				            		if (g.equals("Fantasy")) {
				                		_value.set("M|" + splitter[i-1]);
				                		context.write( _key, _value);
				            		}
				        	}
				        	cnt = 0;
				    	}
				    	cnt++;
				}
			}
            		else {
                		String tmp_value = "";
                		int cnt = 0;
			    	for (int i=0; i < splitter.length; i++) {
			    		if (cnt == 0) {
				        	tmp_value = "R|";
				    	}
				    	else if (cnt == 1) {
		                		_key.set(splitter[i]);
		            		}
		            		else if(cnt == 2) {
		                		tmp_value += splitter[i];
		            		}
		            		else if(cnt == 3) {
		                		_value.set(tmp_value);
		                		context.write(_key, _value);
		                		cnt = -1;
		                		tmp_value = "";
		            		}
		            		cnt++;
                		}
            		}
        	}
        	protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "/movies.txt" ) != -1 ) fileM = true;
			else fileM = false;
		}
     	}

	public static class IMDBReducer extends Reducer<Text,Text,Text,DoubleWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text reduce_key = new Text();
			DoubleWritable reduce_result = new DoubleWritable();
			String title = "";
            double sum = 0;
            int cnt = 0;
			
			for (Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), "|");
				String file_type = itr.nextToken().trim();

				if( file_type.equals("M") ) {
                    title = itr.nextToken().trim();
				}
				else {
                    int rating = Integer.parseInt(itr.nextToken().trim());
                    sum += rating;
                    cnt++;
				}
            }
            reduce_key.set(title);
            reduce_result.set(sum / cnt);
            context.write(reduce_key, reduce_result);
		}
	}

    public static class Data {
        public String title;
        public double rate;

        public Data(String _title, double _rate) {
            this.title = _title;
            this.rate = _rate;
        }

        public String getString() {
            return title + " " + rate;
        }
    }

    public static class DataComparator implements Comparator<Data> {
		public int compare(Data x, Data y) {
			if ( x.rate > y.rate ) return 1;
			if ( x.rate < y.rate ) return -1;
			return 0;
		}
	}

	public static void insertData(PriorityQueue q, String title, double rate, int topK) {
		Data data_head = (Data) q.peek();
		if ( q.size() < topK || data_head.rate < rate ) {
			Data data = new Data(title, rate);
			q.add( data );
			if( q.size() > topK ) 
                q.remove();
		}
	}

    public static class TopKMapper extends Mapper<Text, DoubleWritable, Text, NullWritable> {
		private PriorityQueue<Data> queue ;
        private Comparator<Data> comp = new DataComparator();
		private int topK;
		
		public void map(Object key, Text value, Context context) throws IOException,
		InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String title = itr.nextToken().trim();
            double rate = Double.parseDouble(itr.nextToken().trim());
            insertData(queue, title, rate, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Data>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				Data data = (Data) queue.remove();
				context.write( new Text( data.getString() ), NullWritable.get() );
			}
		}
	}

    public static class TopKReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
		private PriorityQueue<Data> queue ;
		private Comparator<Data> comp = new DataComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(values.toString());
			String title = itr.nextToken().trim();
			    double rate = Double.parseDouble(itr.nextToken().trim());
			    insertData(queue, title, rate, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Data>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				Data data = (Data) queue.remove();
				context.write( new Text( data.getString() ), NullWritable.get() );
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
        	//String first_phase_result = "/first_phase_result";
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        	int topK = Integer.parseInt(otherArgs[2]);
        	
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDB <in> <out> <topK>");
			System.exit(3);
		}
        	conf.setInt("topK", topK);

		Job job1 = new Job(conf, "imdb1");
		job1.setJarByClass(IMDBStudent20180950.class);
		job1.setMapperClass(IMDBMapper.class);
		job1.setReducerClass(IMDBReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		//FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		//FileSystem.get(job1.getConfiguration()).delete( new Path(first_phase_result), true);
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		FileSystem.get(job1.getConfiguration()).delete( new Path(otherArgs[1]), true);
		//job1.waitForCompletion(true);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);



		/**Job job2 = new Job(conf, "imdb2");
		job2.setJarByClass(IMDBStudent20180950.class);
		job2.setMapperClass(TopKMapper.class);
		job2.setNumReduceTasks(1);
		job2.setReducerClass(TopKReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		FileSystem.get(job2.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);**/
	}
}
