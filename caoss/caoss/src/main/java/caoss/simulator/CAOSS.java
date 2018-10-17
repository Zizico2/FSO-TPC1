package caoss.simulator;

import caoss.simulator.configuration.Hardware;
import caoss.simulator.os.FSOOS;
import caoss.simulator.os.OperatingSystem;

/**
 * Entry class for the CAOSS simulator
 * 
 * @author Herve Paulino
 *
 */
public class CAOSS {
	
	public static final Class<? extends OperatingSystem> operatingSystemClass = 
			FSOOS.class;
	
	public static void main(String[] args) {
		Hardware.clock.start();
		OperatingSystem.getInstance().load();		
	}
	
}
