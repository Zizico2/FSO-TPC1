package caoss.simulator.os;

import java.util.Stack;

import caoss.simulator.Program;
import caoss.simulator.hardware.CPUCore;
import caoss.simulator.instructions.Instruction;
import caoss.simulator.instructions.InstructionList;
import caoss.simulator.instructions.InstructionList.InstructionListIterator;

/**
 * A context (thread execution state) 
 * 
 * @author Herve
 *
 */
public class Context {
	
	/**
	 * CPU core where the context is running
	 */
	private CPUCore cpuCore;
	
	/**
	 * List of instructions of the current codeblock
	 */
	private InstructionListIterator instructionList;
	
	/**
	 * A stack of codeblocks for loop and subroutine support
	 */
	private Stack<InstructionListIterator> stack = new Stack<InstructionListIterator>();

	/**
	 * Construct a context from a program
	 * @param program The program
	 */
	protected Context(Program program) {
		this.instructionList = program.iterator();
	}

	/**
	 * Next instruction to execute
	 * @return The instruction
	 */
	public Instruction nextInstruction() {
		return this.instructionList.next();	
	}

	/**
	 * Set the core where the context is running
	 * @param cpuCore The CPU core
	 */
	public void setCPUCore(CPUCore cpuCore) {
		this.cpuCore = cpuCore;
	}

	/**
	 * Get the core where the context is running
	 * @return The CPU core
	 */
	public CPUCore getCpuCore() {
		return cpuCore;
	}

	/**
	 * Jump to a list of instructions
	 * @param instructionList The target list of instructions
	 */
	public void jump(InstructionList instructionList) {
		this.stack.push(this.instructionList);
		this.instructionList = instructionList.iterator();
	}

	/**
	 * Restart the execution of the current Loop 
	 */
	public void loop() {
		this.instructionList.restart();
		
	}
	
	/**
	 * Jump back to the previous codeblock
	 */
	public void jumpBack() {
		this.instructionList = this.stack.pop();
	}
}
