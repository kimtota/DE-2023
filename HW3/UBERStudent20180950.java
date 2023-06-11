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
import java.time.*;
import java.time.format.TextStyle;

public class UBERStudent20180950
{
	public static void main(String[] args) throws Exception
	{
		if (args.length < 1) {
			System.err.println("Usage: UBER <in-file> <out-file>");
			System.exit(1);	
		}
		SparkSession spark = SparkSession
			.builder()
			.appName("UBER")
			.getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		
		PairFunction<String, String, String> pfA = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				StringTokenizer itr = new StringTokenizer(s, ",");
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
				
				String region_day = region + "," + n_day;
				String trips_vehicles = trips + "," + vehicles;
				return new Tuple2(region_day, trips_vehicles);
			}
		};
		JavaPairRDD<String, String> kvpair = lines.mapToPair(pfA);

		Function2<String, String, String> f2 = new Function2<String, String, String>() {
			public String call(String x, String y) {
				StringTokenizer itr = new StringTokenizer(x, ",");
				int x_trip = Integer.parseInt(itr.nextToken().trim());
				int x_vehi = Integer.parseInt(itr.nextToken().trim());

				itr = new StringTokenizer(y, ",");
				int y_trip = Integer.parseInt(itr.nextToken().trim());
				int y_vehi = Integer.parseInt(itr.nextToken().trim());

				int sumTrip = x_trip + y_trip;
				int sumVehi = x_vehi + y_vehi;
				String sum = sumTrip + "," + sumVehi;
				return sum;
			}
		};
		JavaPairRDD<String, String> counts = kvpair.reduceByKey(f2);
		
		counts.saveAsTextFile(args[1]);
		spark.stop();
	}
}
