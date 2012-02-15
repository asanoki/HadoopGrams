package org.jovislab.tools.hadoop.hadoopgrams;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	static enum Counters {
		NUM_DISTINCT_GRAMS_AT_REDUCER_BEFORE_THRESHOLD,
		NUM_DISTINCT_GRAMS_AT_REDUCER_AFTER_THRESHOLD,
	}

	protected int configThreshold;
	protected Counter counterBefore;
	protected Counter counterAfter;

	protected void localSetup(Context context) {
		Configuration config = context.getConfiguration();
		configThreshold = config.getInt("reduce.threshold", 0);
	}
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		counterBefore = context.getCounter(Counters.NUM_DISTINCT_GRAMS_AT_REDUCER_BEFORE_THRESHOLD);
		counterAfter = context.getCounter(Counters.NUM_DISTINCT_GRAMS_AT_REDUCER_AFTER_THRESHOLD);
		localSetup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		counterBefore.increment(1);
		if (sum >= configThreshold) {
			LongWritable result = new LongWritable(sum);
			counterAfter.increment(1);
			context.write(key, result);
		}
	};

}
