package com.lifexweb.app.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountMainTest {

	private WordCountMapper mapper;
	private WordCountReducer reducer;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable > driver;
	
	@Before
	public void setUp() throws Exception {
		mapper = new WordCountMapper();
		reducer = new WordCountReducer();
		driver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable > (mapper, reducer);
	}

	@Test
	public final void testMain() {
		driver.withInput(new LongWritable(0), new Text("word.count, test Test  TEST"))
		.withOutput(new Text("count"), new IntWritable(1))
		.withOutput(new Text("test"), new IntWritable(3))
		.withOutput(new Text("word"), new IntWritable(1))
		.runTest();
	}

}
