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

public class Data {
	public String title;
	public double rate;

	public Data(String _title, double _rate) {
		   this.title = _title;
		   this.rate = _rate;
	}

	public String getString() {
		   return title + "|" + rate;
	}
	
	public String getString2() {
		return title + " " + rate;
	}
 }
