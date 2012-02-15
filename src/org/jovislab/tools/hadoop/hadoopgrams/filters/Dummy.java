package org.jovislab.tools.hadoop.hadoopgrams.filters;

import org.jovislab.tools.hadoop.hadoopgrams.Filter;

public class Dummy implements Filter {
	public String filter(String input) {
		return input;
	}
}
