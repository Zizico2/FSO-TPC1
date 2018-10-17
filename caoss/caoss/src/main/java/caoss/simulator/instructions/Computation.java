package caoss.simulator.instructions;

import caoss.simulator.hardware.CPUCore;

/**
 * An instruction that represents a certain amount of computing clock cycles
 * 
 * @author Herve Paulino
 *
 */
public class Computation implements Instruction {
	
	/**
	 * The number of CPU clock cycles the instruction takes to execute
	 */
	private final int cpuClocks;
	
	/**
	 * Construct a computation instruction 
	 * @param cpuClocks  The number of CPU clock cycles the instruction takes to execute
	 */
	public Computation(int cpuClocks) {
		this.cpuClocks = cpuClocks;
	}

	@Override
	public void run (CPUCore cpuCore) {
		//System.out.println(this);
	}

	@Override
	public int getCPUClocks() {
		return cpuClocks;
	}
	
	@Override
	public String toString() {
		return "Computation " + cpuClocks;
	}

}
