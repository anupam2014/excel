package com.durga.mnrao.excel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExcelRecordReader extends RecordReader<LongWritable, Text> {
	
	
	String [] records;
	
	LongWritable key=new LongWritable() ;
	
	Text  value=null;;
	
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
				
		FileSplit fileSplit = (FileSplit)inputSplit;
		
		Path path = fileSplit.getPath();
		
		Configuration conf = context.getConfiguration();
		
		FileSystem fs = path.getFileSystem(conf);
		
		FSDataInputStream input = fs.open(path);
		
		
		ExcelParser  parser = new ExcelParser();
		
		String excelData = parser.parseExcelData(input);	
		
		 records = excelData.split("\n");		
		
	}
	
	
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		
			if(records.length==0)			
			{
				return false;	
			}
			
			if(key.get()<records.length)
			{
				int index = (int) key.get();
				
				String currentRecord = records[index];
				
				value= new Text(currentRecord);	
				
				long currentKey = key.get() + 1;
				
				key.set(currentKey);
				
				return true;
				
			}
			else
			{
				return false;
			}
					
	}

	
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}
		

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	

}
