package com.apache.hadoop.bdpp;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class fnalmapper extends Mapper<Object, Text, Text, NullWritable> 
{
	// Stores a map of user reputation to the record
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
	public void map(Object key, Text mapperInputValue, Context context)
	throws IOException, InterruptedException 
	{
		String line = mapperInputValue.toString();
	    String[] values = line.split("\t");
		Configuration conf = context.getConfiguration();
		repToRecordMap.put(Integer.parseInt(values[1]), new Text(values[0]));
		// If we have more than ten records, remove the one with the lowest rep
		// As this tree map is sorted in ascending order, the user with
		// the highest reputation is the last key.
		if (repToRecordMap.size() > 1) 
		{
		
			repToRecordMap.remove(repToRecordMap.firstKey());
		}
	}
	protected void cleanup(Context context) throws IOException,
	InterruptedException 
	{
		 Set keys = repToRecordMap.keySet();
		   for (Iterator i = keys.iterator(); i.hasNext();) 
		   {
		     int key = (Integer) i.next();
		     Text value =  repToRecordMap.get(key);
		     context.write(value, NullWritable.get());
		     //System.out.println(key + " = " + value);
		   }
		
}
}
