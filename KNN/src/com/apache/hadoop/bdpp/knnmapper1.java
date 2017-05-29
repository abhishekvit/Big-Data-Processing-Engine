package com.apache.hadoop.bdpp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class knnmapper1 extends
Mapper<LongWritable, Text, Text, IntWritable>{
	int c=0;
	public void map(LongWritable mapperInputKey, Text mapperInputValue,
			Context context) throws IOException, InterruptedException 
	{
		String line = mapperInputValue.toString();
		//line = line.replaceAll("'", ""); // remove single quotes (e.g., can't)
	    //line = line.replaceAll("[^a-zA-Z0-9]", " "); // replace the rest with a space

		String[] values = line.split("\t");
		context.write(new Text(values[1]), new IntWritable(1));
		
	}

}
