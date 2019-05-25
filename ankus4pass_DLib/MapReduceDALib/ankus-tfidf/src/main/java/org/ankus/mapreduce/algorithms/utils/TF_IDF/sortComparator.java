package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class sortComparator extends WritableComparator {

	 protected sortComparator() {
	  super(DoubleWritable.class, true);
	  // TODO Auto-generated constructor stub
	 }

	 @Override
	 public int compare(WritableComparable o1, WritableComparable o2) {
	  DoubleWritable k1 = (DoubleWritable) o1;
	  DoubleWritable k2 = (DoubleWritable) o2;
	  int cmp = k1.compareTo(k2);
	  return -1 * cmp;
	 }
}
