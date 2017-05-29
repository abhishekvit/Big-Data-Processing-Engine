package com.apache.hadoop.bdpp;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class knnmapper extends Mapper<Object, Text, DoubleWritable, Text> 
{
	// Stores a map of user reputation to the record
	int c=0;
	private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
	public void map(Object key, Text mapperInputValue, Context context)
	throws IOException, InterruptedException 
	{
		if(c>0)
		{
			String line = mapperInputValue.toString();
			String[] values = line.split(",");
			Configuration conf = context.getConfiguration();
			String params = conf.get("params");
			params=params.substring(1);
			//System.out.println("params"+params);
			String parameters[]=params.split(",");
			double distance=0;
			int x,xi;
			int nattr=Integer.parseInt(parameters[0]);
			//System.out.println("nattr "+nattr);
			int k=Integer.parseInt(parameters[1]);
			for(int i=2;i<parameters.length;i++)
			{
				x=Integer.parseInt(parameters[i]);
				xi=Integer.parseInt(values[i-2]);
				System.out.println("x "+x+" xi "+xi);
				distance=distance+((xi-x)*(xi-x));
			}
			System.out.println(distance);
			distance=Math.sqrt(distance);
			repToRecordMap.put(distance,new Text(values[nattr]));
			System.out.println(distance+" "+values[nattr]);
			if (repToRecordMap.size() > k) 
			{
				repToRecordMap.remove(repToRecordMap.lastKey());
			}
		}
		++c;
	}
	protected void cleanup(Context context) throws IOException,
	InterruptedException 
	{
		if(c>0)
		{ 
			Set keys = repToRecordMap.keySet();
			for (Iterator i = keys.iterator(); i.hasNext();) 
			{
				double key = (Double) i.next();
				Text value =  repToRecordMap.get(key);
				context.write(new DoubleWritable(key), value);
				System.out.println(key + " = " + value);
			}
		 }
	}
}