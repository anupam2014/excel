package com.durga.mnrao.excel;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExcelMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
	
	String record = value.toString();
	
	String[] fields = record.split(",");
	
	context.write(new Text(fields[2]), new LongWritable(1) );
	
	}
}
