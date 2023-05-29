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

public class YouTubeStudent20180950
{
    public static class YouTubeMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        private Text _key = new Text();
        private DoubleWritable _value = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
            StringTokenizer itr = new StringTokenizer(value.toString(), "|");
            String serial_num = itr.nextToken().trim();
            String company = itr.nextToken().trim();
            String num = itr.nextToken().trim();
            String category = itr.nextToken().trim();
            String num2 = itr.nextToken().trim();
            String num3 = itr.nextToken().trim();
            Double rate = Double.parseDouble(itr.nextToken().trim());

            _key.set(category);
            _value.set(rate);
            context.write(_key, _value);
        }
     }

	public static class YouTubeReducer extends Reducer<Text,DoubleWritable,Text,Text>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			Text _key = new Text();
			Text reduce_result = new Text();
            		double sum = 0;
            		double cnt = 0;
			
			for (DoubleWritable val : values) {
				double num = val.get();
                		sum += num;
                		cnt += 1;
            		}
            		sum = sum / cnt;
            		String odf = String.format("%.4f", sum);
            		_key.set(key + "|");
            		reduce_result.set(odf);
            		context.write(_key, reduce_result);
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

    public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable> {
		private PriorityQueue<Data> queue ;
        private Comparator<Data> comp = new DataComparator();
		private int topK;
		
		public void map(Object key, Text value, Context context) throws IOException,
		InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
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
			StringTokenizer itr = new StringTokenizer(key.toString(),"|");
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
				context.write( new Text( data.getString2() ), NullWritable.get() );
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
        	String first_phase_result = "/first_phase_result";
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        	int topK = Integer.parseInt(otherArgs[2]);
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: Youtube <in> <out>");
			System.exit(3);
		}
        	conf.setInt("topK", topK);

		Job job1 = new Job(conf, "youtube1");
		job1.setJarByClass(YouTubeStudent20180950.class);
		job1.setMapperClass(YouTubeMapper.class);
		job1.setReducerClass(YouTubeReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		FileSystem.get(job1.getConfiguration()).delete( new Path(first_phase_result), true);
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "youtube2");
		job2.setJarByClass(YouTubeStudent20180950.class);
		job2.setMapperClass(TopKMapper.class);
		job2.setNumReduceTasks(1);
		job2.setReducerClass(TopKReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);

        	FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		FileSystem.get(job2.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
