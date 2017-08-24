package com.durga.mnrao.excel;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
//import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
//import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;

	public class ExcelParser 
	{

		private static final Log LOG = LogFactory.getLog(ExcelParser.class);
		
		private StringBuilder currentString = null;
		private long bytesRead = 0;

		public String parseExcelData(InputStream is) {
			
			try {
				//to get reference of Work book
				XSSFWorkbook workbook = new XSSFWorkbook(is);
				
				XSSFSheet sheet = workbook.getSheetAt(0);
							
				// 0 ---> for the first sheet
				// 1 ----> for the second sheet
				// 2 ---> for the third sheet
						

				// Iterate through each rows from first sheet
				Iterator<Row> rowIterator = sheet.rowIterator();
				
				currentString = new StringBuilder();
				
				
				XSSFRow row;
				XSSFCell cell;
								
				while (rowIterator.hasNext()) 
				{
					 row = (XSSFRow)rowIterator.next();
					
					// For each row, iterate through each columns
					@SuppressWarnings("rawtypes")
					Iterator  cellIterator = row.cellIterator();
					
					while (cellIterator.hasNext()) 
					{
						
						
						 cell = (XSSFCell)cellIterator.next();

						switch (cell.getCellType()) 
						{
						
						case XSSFCell.CELL_TYPE_BOOLEAN:
							bytesRead++;
							currentString.append(cell.getBooleanCellValue() + ",");
							break;

						case XSSFCell.CELL_TYPE_NUMERIC:
							bytesRead++;
							currentString.append(cell.getNumericCellValue() + ",");
							break;

						case XSSFCell.CELL_TYPE_STRING:
							bytesRead++;
							currentString.append(cell.getStringCellValue() + ",");
							break;

						}
								
						
					}
					
					currentString.setLength(currentString.length() - 1);
					currentString.append("\n");
				}
				
				is.close();
			} catch (IOException e) {
				LOG.error("IO Exception : File not found " + e);
			}
			//System.out.println("String builder : "+currentString.length()+"vaue  "+currentString);
			String temp = currentString.toString();
			return temp;
			
			
		}
	
		public long getBytesRead() {
			return bytesRead;
		}

}
