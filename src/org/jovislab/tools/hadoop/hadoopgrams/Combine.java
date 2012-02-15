package org.jovislab.tools.hadoop.hadoopgrams;

import org.apache.hadoop.conf.Configuration;

public class Combine extends Reduce {
	@Override
	protected void localSetup(Context context) {
		Configuration config = context.getConfiguration();
		configThreshold = config.getInt("combine.threshold", 0);
	}
}
