package caoss.simulator.instructions;

/**
 * Base class for a memory access instruction
 * It factorizes many of the operations common to load and store operations
 * 
 * @author Herve Paulino
 *
 */
public abstract class MemoryAccess implements Instruction {
	
	/**
	 * The memory address upon which the operation is performed
	 */
	protected final long address;
	
	/**
	 * Construct the memory access operation
	 * @param address The address upon which the operation is performed
	 */
	public MemoryAccess(long address) {
		this.address = address;
	}

	@Override
	public int getCPUClocks() {
		return 1;
	}
}
