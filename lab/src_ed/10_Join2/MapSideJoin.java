/*import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapSideJoin 
{
	public static class MapSideJoinMapper extends Mapper<Object, Text, Text, Text>{
		Hashtable<String,String> joinMap = new Hashtable<String,String>();
		
		public void map(Object key, Text value, Context context) throws IOException,
		InterruptedException{
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles( context.getConfiguration() );
			BufferedReader br = new BufferedReader( new FileReader( cacheFiles[0].toString() ) );
			String line = br.readLine();
			
			while( line != null ) {
				StringTokenizer itr = new StringTokenizer(line, "|");
				String category = itr.nextToken();
				String category_name = itr.nextToken();
				joinMap.put( category, category_name );
				line = br.readLine();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MapSideJoin <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "MapSideJoin");
		DistributedCache.addCacheFile( new URI( "/join_data/relation_b" ), job.getConfiguration() ); //주소수정
	}
}*/
