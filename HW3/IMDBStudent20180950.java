import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.IOException;
import java.util.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


public class IMDBStudent20180950
{
	public static void main(String[] args) throws Exception
	{
		if (args.length < 2) {
			System.err.println("Usage: IMDB <in-file> <out-file>");
			System.exit(1);
		}
		SparkSession spark = SparkSession
			.builder()
			.appName("IMDB")
			.getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		
		FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				Iterator<String> list = Arrays.asList(s.split("::")).iterator();
				int cnt = 0;
				while(list.hasNext()){
					if(cnt != 3) {
						list.next();
						list.remove();
						cnt++;
					}
					else {
						list.next();
						cnt = 0;
					}
				}
				return list;
			}
		};
		JavaRDD<String> words1 = lines.flatMap(fmf);
		
		FlatMapFunction<String, String> fmf2 = new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				return Arrays.asList(s.split("|")).iterator();
			}
		};
		JavaRDD<String> words2 = words1.flatMap(fmf2);
		
		PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s, 1);
			}
		};
		JavaPairRDD<String, Integer> ones = words2.mapToPair(pf);
		
		Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		};
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
		
		counts.saveAsTextFile(args[1]);
		spark.stop();
		
		/*
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "imdb");
		job.setJarByClass(IMDBStudent20180950.class);
		job.setMapperClass(IMDBMapper.class);
		job.setCombinerClass(IMDBReducer.class);
		job.setReducerClass(IMDBtReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		*/
	}
}
