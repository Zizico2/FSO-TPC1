package caoss.simulator.instructions;

import caoss.simulator.hardware.CPUCore;

/**
 * Class representing a memory load instruction
 * 
 * @author Herve Paulino
 *
 */
public class MemoryLoad extends MemoryAccess {
	
	/**
	 * Construct the memory load operation
	 * @param address The source address
	 */
	public MemoryLoad(long address) {
		super(address);
	}
	
	@Override
	public void run (CPUCore cpuCore) {
		System.out.println(this);
	}
	
	@Override
	public String toString() {
		return "Load from memory address " + address;
	}

}
