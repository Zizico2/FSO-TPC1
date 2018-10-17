package caoss.simulator.instructions;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import caoss.simulator.AbstractCAOSSTest;

public class InvalidSysCallTest extends AbstractCAOSSTest {

	@Override
	protected String getCAOSSFileName() {
		return "invalid_syscall.caoss";
	}

	@Test
	public void test() {
		try {
			compile();
		} catch (Exception e) {
			if (e instanceof NoSuchSysCallException) 
				assertTrue(true);
			else {
				System.err.println(e.getLocalizedMessage());
				fail();	
			}
		}
	}
}
