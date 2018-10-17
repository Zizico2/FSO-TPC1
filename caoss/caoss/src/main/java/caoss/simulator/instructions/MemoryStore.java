package caoss.simulator.instructions;

import caoss.simulator.hardware.CPUCore;

/**
 * Class representing a memory store instruction
 * 
 * @author Herve Paulino
 *
 */
public class MemoryStore extends MemoryAccess {
	
	/**
	 * Construct a memory store operation
	 * @param address Target address
	 */
	public MemoryStore(long address) {
		super(address);
	}
	
	@Override
	public void run (CPUCore cpuCore) {
		System.out.println(this);
	}
	
	@Override
	public String toString() {
		return "Store to memory address " + address;
	}

}
