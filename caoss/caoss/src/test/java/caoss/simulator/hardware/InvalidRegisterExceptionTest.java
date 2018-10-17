package caoss.simulator.hardware;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import caoss.simulator.AbstractCAOSSTest;
import caoss.simulator.console.ExecCommand;

public class InvalidRegisterExceptionTest extends AbstractCAOSSTest {

	@Override
	protected String getCAOSSFileName() {
		return "invalid_register_exception.caoss";
	}

	@SuppressWarnings("serial")
	@Test
	public void test() {
		
			new Clock().start();
			new ExecCommand().run(new ArrayList<String>() {{ add(getAbsoluteFileName()); }});
			assertTrue(true);
	}

}
