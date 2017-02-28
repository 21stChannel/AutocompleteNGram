//import com.sun.xml.internal.ws.api.ha.StickyFeature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			Configuration conf1 = context.getConfiguration();
			threshold = conf1.getInt("threshold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) return;
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) return;
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.parseInt(wordsPlusCount[1]);
			if (count < threshold) return; //filter phrases that are not popular

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; ++i) {
				sb.append(words[i]).append(" ");
			}

			String outputKey = sb.toString().trim();
			String outputValue = words[words.length-1] + "=" + count;
			context.write(new Text(outputKey), new Text(outputValue));
			//mapper data temporary output will be stored on hdfs
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topK;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", 5);
			//how many autocomplete to store
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeMap<Integer, List<String>> tMap = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
			for (Text v : values) {
				String val = v.toString().trim();
				String[] tmp = val.split("=");
				String word = tmp[0].trim();
				int count = Integer.parseInt(tmp[1].trim());
				if (!tMap.containsKey(count)) tMap.put(count, new ArrayList<String>());
				tMap.get(count).add(word);
			}

			int kCount = 0;
			for (int cnt : tMap.keySet()) {
				if (kCount == topK) return;
				List<String> phrases = tMap.get(cnt);
				for (String s : phrases) {
					if (kCount == topK) return;
					context.write(new DBOutputWritable(key.toString(), s, cnt), NullWritable.get());
					++kCount;
				}
			}
			//or use PQ
		}
	}
}
