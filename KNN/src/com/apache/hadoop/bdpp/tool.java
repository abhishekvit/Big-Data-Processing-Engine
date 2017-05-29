package com.apache.hadoop.bdpp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class tool extends Configured implements Tool 
{
	private static final String OUTPUT_PATH = "intermediate_output3";
	private static final String OUTPUT_PATH2 = "intermediate_output4";
	public static void main(String[] args) throws Exception 
	{
		System.out.println("************************** In main method ****************");
		Configuration conf = new Configuration();
		tool dateTR = new tool();
		int l=args.length;
		String params="";
		String nattr;
		for(int i=2;i<l;i++)
		{
			params=params+","+args[i];
		}
		conf.set("params", params);
		Job job = new Job(conf);
		int exitCode = ToolRunner.run(conf, dateTR, args);
		System.exit(exitCode);
	}
	public int run(String[] arg0) throws Exception 
	{
		Configuration conf = getConf();
		if (arg0.length == 2) 
		{
			System.err.println("pass number of attributes, value of k, value of one attribute");
			System.exit(2);
		}
		if (arg0.length == 3) 
		{
			System.err.println("pass value of k, value of one attribute");
			System.exit(2);
		}
		if (arg0.length == 4) 
		{
			System.err.println("pass value of one attribute");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf,"Generalised K Nearest Neighbour");
		job.setNumReduceTasks(0);
		System.out.println("************************** entering knnmapper ****************");
		System.out.println("number of nearest neighbours which are being considered "+arg0[3]);
		job.setJarByClass(tool.class);
		job.setMapperClass(knnmapper.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		
		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(OUTPUT_PATH);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);

		  /*
		   * Job 2
		   */
		 System.out.println("************************** entering knnmapper1 ****************");
		 Job job2 = new Job(conf, "Job 2");
		 job2.setJarByClass(tool.class);
		 job2.setMapperClass(knnmapper1.class);
		 job2.setReducerClass(knnreeucer1.class);
		 job2.setOutputKeyClass(Text.class);
		 job2.setOutputValueClass(IntWritable.class);
		 Path inPath = new Path(OUTPUT_PATH);
		 Path outPath = new Path(OUTPUT_PATH2);
		 FileInputFormat.addInputPath(job2, inPath);
		 FileOutputFormat.setOutputPath(job2, outPath);
		 job2.waitForCompletion(true);
		 
		 /*
		   * Job 3
		   */
		 
		 System.out.println("************************** entering fnalmapper ****************");
		 Job job3 = new Job(conf, "Job 3");
		 job3.setJarByClass(tool.class);
		 job3.setMapperClass(fnalmapper.class);
		 //job3.setNumReduceTasks(0);
		 job3.setOutputKeyClass(Text.class);
		 job3.setOutputValueClass(NullWritable.class);
		 Path inPath1 = new Path(OUTPUT_PATH2);
		 Path outPath1 = new Path(arg0[1]);
		 FileInputFormat.addInputPath(job3, inPath1);
		 FileOutputFormat.setOutputPath(job3, outPath1);
		 return job3.waitForCompletion(true) ? 0 : 1;
		
	}

}

