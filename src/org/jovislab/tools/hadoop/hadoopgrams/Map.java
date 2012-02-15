package org.jovislab.tools.hadoop.hadoopgrams;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jovislab.tools.hadoop.hadoopgrams.filters.Dummy;

public class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
	static enum Counters {
		NUM_GRAMS_BEFORE_FILTERING, NUM_GRAMS_AFTER_FILTERING
	}

	private final static LongWritable one = new LongWritable(1);
	private Text word = new Text();
	private Filter filter = new Dummy();
	private int configN;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		@SuppressWarnings("unchecked")
		Class<Filter> filterClass = (Class<Filter>) config.getClass(
				"map.filter", null);
		if (filterClass != null) {
			try {
				filter = filterClass.newInstance();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		configN = config.getInt("n", 2);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		String processed = filter.filter(value.toString());
		int size = processed.length();
		context.getCounter(Counters.NUM_GRAMS_BEFORE_FILTERING).increment(
				value.getLength() - configN);
		context.getCounter(Counters.NUM_GRAMS_AFTER_FILTERING).increment(
				size - configN);
		for (int i = 0; i < size - configN; i++) {
			word.set(processed.substring(i, i + configN));
			context.write(word, one);
		}
	};
}
