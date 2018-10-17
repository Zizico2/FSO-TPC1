package caoss.simulator.instructions;

import caoss.simulator.hardware.CPUCore;

/**
 * Class representing a "loop" instruction
 * 
 * @author Herve Paulino
 *
 */
public class Loop implements Instruction {
	
	/**
	 * Total number of iterations
	 */
	private final int numIterations;
	
	/**
	 * Number of remaining iterations
	 */
	private int remainingIterations;
	
	/**
	 * The body of the loop
	 */
	private final InstructionList instructionList;
	
	/**
	 * Constructor
	 * @param numIterations Number of iterations
	 * @param instructionList Body of the loop
	 */
	public Loop(int numIterations, InstructionList instructionList) {
		if (numIterations < 0)
			throw new IllegalArgumentException("Illegal number of iterations: " + numIterations);
		
		this.remainingIterations = this.numIterations = numIterations;
		this.instructionList = instructionList;
		if (this.numIterations > 0) {
			this.instructionList.add(this);
		}
	}

	@Override	
	public final void run(CPUCore cpuCore) {
		if (this.numIterations != 0) {	
			if (this.remainingIterations == this.numIterations) {
				// First iteration
				cpuCore.getContext().jump(instructionList);
				this.remainingIterations--;
			}
			else if (this.remainingIterations == 0) {
				// Last iteration
				cpuCore.getContext().jumpBack();
				this.remainingIterations = this.numIterations; // reset the loop for nested calls
			}
			else {
				cpuCore.getContext().loop();
				this.remainingIterations--;
			}
		}
 	}

	@Override
	public int getCPUClocks() {
		return 1;
	}
	
	@Override
	public String toString() {
		String result = "loop " + this.numIterations + " {\n" ;
		for (int i = 0; i < this.instructionList.size()-1; i++)
			result += "\t" + this.instructionList.get(i).toString() + "\n";
		result += "\n}";
		return result;		
	}

}
