#!/bin/sh

INPUT=/user/fedora/jawiki.txt
OUTPUT=/user/fedora/ngrams.txt

bin/hadoop dfs -rmr $OUTPUT
bin/hadoop jar ~/workspace/HadoopGrams/dist/HadoopGrams.jar org.jovislab.tools.hadoop.hadoopgrams.HadoopGrams \
	$INPUT $OUTPUT 3 org.jovislab.tools.hadoop.hadoopgrams.filters.Wp2TxtDump 0 2
