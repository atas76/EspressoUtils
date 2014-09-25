package org.espressodb.util.loader;

import java.util.List;

import org.espressodb.core.ColumnGroup;
import org.espressodb.core.Database;

public class DatabaseFactory {
	
	public static Database createDatabase(String name, List<ColumnGroup> colGroupList) {
	
		Database db = new Database(name);
		
		for (ColumnGroup colGroup:colGroupList) {
			db.insert(colGroup);
		}
		
		return db;
	}
}
