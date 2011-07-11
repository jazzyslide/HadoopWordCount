package com.lifexweb.app.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		//ドット、カンマはスペースに置き換えて、全部小文字にする
		line = line.replaceAll("\\.", " ")
				.replaceAll("\\,", " ")		
				.toLowerCase();
		
		for (String wordStr : line.split("\\s")){
			if (!wordStr.isEmpty() && wordStr != ""){
				word.set(wordStr);
				context.write(word, ONE);
			}
		}
	}
	
	
}
