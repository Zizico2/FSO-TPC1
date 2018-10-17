package caoss.simulator.parser;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.Test;

import caoss.simulator.AbstractCAOSSTest;
import caoss.simulator.instructions.Instruction;

public class ParserTest extends AbstractCAOSSTest {
	
	@Test
	public void test() {
		try {
			Iterator<Instruction> it = compile().iterator();
			while (it.hasNext())
				System.out.println(it.next());
			
			assertTrue(true);
		} 
		catch (Exception e) {
			System.out.println(e.getMessage());
			fail();
		}
		
	}

	@Override
	protected String getCAOSSFileName() {
		return "simple.caoss";
	}

}
