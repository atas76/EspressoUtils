package org.espressodb.util.loader.impl;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

public class SjonColumnGroupLoaderTest {
	
	private final static String DATASOURCE_ROOT = "./resources/LeagueHistory";
	
	private SjonColumnGroupLoader directoryLoader;
	private SjonColumnGroupLoader fileLoader;
	
	@Before
	public void setUp() throws Exception {
		this.directoryLoader = new SjonColumnGroupLoader(new File(DATASOURCE_ROOT));
		this.fileLoader = new SjonColumnGroupLoader(new File(DATASOURCE_ROOT + "/table2013-14.sjon"));
	}
	
	@Test
	public void testDirectoryLoader() {
		assertEquals(10, this.directoryLoader.sjonFiles.size());
	}
	
	@Test
	public void testFileLoader() {
		assertEquals(1, this.fileLoader.sjonFiles.size());
		assertEquals("table2013-14.sjon", this.fileLoader.sjonFiles.get(0).getName());
	}
	
	@Test
	public void testLoad() throws Exception {
		
		this.directoryLoader.load();
		this.fileLoader.load();
		
		assertEquals(440, this.directoryLoader.getColumnGroups().size());
		assertEquals(44, this.fileLoader.getColumnGroups().size());
	}
}
