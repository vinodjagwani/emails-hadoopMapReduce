package com.mapreduce.mailarch;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MailAchive {

	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		private Text lines = new Text();
		private Text addLine = new Text("_______________________________");
		int i = 1;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (line.matches("(^Date:.*)|(^From:.*)|(^To:.*)|(^Subject:.*)")) {
				lines.set(i+"	"+line);
				context.write(new IntWritable(), lines);
				i++;
				context.write(new IntWritable(), addLine);
			}
		}
	}

	public static class Reduce extends Reducer<LongWritable, Text, IntWritable, Text> {
		public void reduce(Text key, Text values, Context context)throws IOException, InterruptedException {
			context.write(new IntWritable(), key);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		Job job = new Job(conf, "emailarchive");
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);
		//FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

	}

}