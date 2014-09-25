package org.espressodb.util.loader;

import java.util.List;

import org.espressodb.core.ColumnGroup;
import org.espressodb.util.loader.exceptions.LoadException;

public interface ColumnGroupLoader {
	
	public List<ColumnGroup> getColumnGroups();
	public void load() throws LoadException;

}
