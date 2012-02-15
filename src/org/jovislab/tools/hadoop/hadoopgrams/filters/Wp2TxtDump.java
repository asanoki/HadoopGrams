package org.jovislab.tools.hadoop.hadoopgrams.filters;

import org.jovislab.tools.hadoop.hadoopgrams.Filter;

/* This filter is to process Wikipedia dumps preprocessed with WP2TXT software */
/* Not just raw dumps in XML format */
public class Wp2TxtDump implements Filter {
	public String filter(String input) {
		if (input.startsWith("[[")) {
			// Remove title
			return input.substring(2, input.length() - 4);
		}
		if (input.startsWith("= ")) {
			// Remove header
			return input.substring(2, input.length() - 4);
		}
		if (input.startsWith("== ")) {
			// Remove header
			return input.substring(3, input.length() - 6);
		}
		if (input.startsWith("=== ")) {
			// Remove header
			return input.substring(4, input.length() - 8);
		}
		if (input.startsWith("==== ")) {
			// Remove header
			return input.substring(5, input.length() - 10);
		}
		if (input.startsWith("===== ")) {
			// Remove header
			return input.substring(6, input.length() - 12);
		}
		if (input.startsWith("====== ")) {
			// Remove header
			return input.substring(7, input.length() - 14);
		}
		if (input.startsWith("======= ")) {
			// Remove header
			return input.substring(8, input.length() - 16);
		}
		if (input.startsWith("======== ")) {
			// Remove header
			return input.substring(9, input.length() - 18);
		}
		if (input.startsWith("- ")) {
			// Remove list indicator
			return input.substring(2, input.length() - 2);
		}
		if (input.startsWith("* ")) {
			// Remove list indicator
			return input.substring(2, input.length() - 2);
		}
		return input;
	}
}
