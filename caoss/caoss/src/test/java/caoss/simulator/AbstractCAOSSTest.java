package caoss.simulator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.junit.Test;

import caoss.simulator.parser.ParseException;
import caoss.simulator.parser.Parser;

public abstract class AbstractCAOSSTest {

	protected static final String testFolder = "src" + File.separator + 
			"test" + File.separator + 
			"resources" + File.separator;
		
	protected abstract String getCAOSSFileName();
	
	@Test
	public abstract void test();
	
	protected Program compile() throws FileNotFoundException, ParseException {
		return new Parser(new FileInputStream(testFolder + getCAOSSFileName())).
				Program(getCAOSSFileName());
		
	}
	
	protected String getAbsoluteFileName() {
		return testFolder + getCAOSSFileName();
	}
}
