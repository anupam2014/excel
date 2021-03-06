package com.durga.mnrao.excel;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExcelReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	@Override
	protected void reduce(Text key , Iterable<LongWritable> values,	Context context) throws IOException, InterruptedException {
		
		
		long sum = 0;
		
		for (LongWritable value : values) {
			
			sum = sum+value.get();
			
		}
		
		context.write(key, new LongWritable(sum));
		
		
	}

}
