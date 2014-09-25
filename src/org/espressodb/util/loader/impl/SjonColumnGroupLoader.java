package org.espressodb.util.loader.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.espressodb.core.Column;
import org.espressodb.core.ColumnGroup;
import org.espressodb.util.loader.ColumnGroupLoader;
import org.espressodb.util.loader.exceptions.SjonLoadException;
import org.espressodb.util.loader.exceptions.UndefinedDataSourceException;
import org.sjon.db.SjonRecord;
import org.sjon.db.SjonTable;
import org.sjon.parser.SjonParsingException;
import org.sjon.parser.SjonScanningException;

public class SjonColumnGroupLoader implements ColumnGroupLoader {
	
	List<File> sjonFiles = new ArrayList<File>();
	List<ColumnGroup> columnGroups = new ArrayList<ColumnGroup>();
	
	/**
	 * 
	 * Loads the input data (the SJON files) into the object
	 * 
	 * @param path either a directory of SJON files or an SJON file itself. In case of directory, the files having the "sjon" suffix are taken into account. No recursion in subsequent directories is supported.
	 * @throws UndefinedDataSourceException in case of the path not being either a file not a directory 
	 */
	public SjonColumnGroupLoader(File path) throws UndefinedDataSourceException {
		
		if (path.isDirectory()) {
			
			File [] files = path.listFiles();
			
			for (File file:files) {
				if (file.isFile() && file.getName().endsWith(".sjon")); {
					sjonFiles.add(file);
				}
			}
		} else if (path.isFile()) {
			sjonFiles.add(path);
		} else {
			throw new UndefinedDataSourceException("SJON data source neither a directory nor a file");
		}
	}
	
	public void load() throws SjonLoadException {
		
		try {
			
			for (File sjonFile:sjonFiles) {
				
				SjonTable sjonTable = new SjonTable(sjonFile.getAbsolutePath());
				List<SjonRecord> sjonRecords = sjonTable.getData();
				
				for (SjonRecord sjonRecord: sjonRecords) {
					
					List<Column> columns = new ArrayList<Column>();
					
					List<String> orderedValues = sjonRecord.getOrderedValues();
					Map<String, String> namedValues = new HashMap<String, String>();
					for (String fieldName: sjonRecord.getFieldNames()) {
						// System.out.println("Adding field name: " + fieldName);
						namedValues.put(fieldName, sjonRecord.getValue(fieldName));
					}
					for (String orderedValue: orderedValues) {
						// System.out.println("Adding ordered value: " + orderedValue);
						columns.add(new Column(orderedValue));
					}
					for (Map.Entry<String, String> namedValue: namedValues.entrySet()) {
						columns.add(new Column(namedValue.getKey(), namedValue.getValue()));
					}
					
					this.columnGroups.add(new ColumnGroup(columns));
				}
			}
		} catch (IOException ioex) {
			throw new SjonLoadException("Error accessing SJON data source");
		} catch (SjonScanningException sjscex) {
			throw new SjonLoadException("Error processing SJON data");
		} catch (SjonParsingException sjpex) {
			throw new SjonLoadException("Error processing SJON data");
		}
	}
	
	public List<ColumnGroup> getColumnGroups() {
		return this.columnGroups;
	}

}
