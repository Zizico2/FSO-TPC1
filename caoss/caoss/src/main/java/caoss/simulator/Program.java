package caoss.simulator;

import caoss.simulator.instructions.Instruction;
import caoss.simulator.instructions.InstructionList;
import caoss.simulator.instructions.InstructionList.InstructionListIterator;
import caoss.simulator.instructions.SysCall;
import caoss.simulator.os.syscalls.SysCallNumber;

/**
 * Class that defines programs to be executed by the CAOSS simulator
 * 
 * @author Herve Paulino
 *
 */
public class Program  {

	/**
	 * The program's file name
	 */
	private final String fileName;
	
	/**
	 * The program's code (list of instructions)
	 */
	private final InstructionList instructionList;

	/**
	 * Are computation instructions to be interpreted.
	 */
	private static boolean isComputationOn = true;

	/**
	 * Are memory access instructions to be interpreted.
	 */
	private static boolean isMemoryAccessOn = true;

	/**
	 * Construct a program from the given list of instructions
	 * @param instructionList The list of instructions
	 */
	public Program(String fileName, InstructionList instructionList) {
		this.fileName = fileName;
		this.instructionList = instructionList;
		Instruction lastInstruction = this.instructionList.get(this.instructionList.size() - 1);
		if ( (! (lastInstruction instanceof SysCall) ) ||
				( (lastInstruction instanceof SysCall) && 
						((SysCall) lastInstruction).getNumber() != SysCallNumber.EXIT)) {
			this.instructionList.add(
					new SysCall(SysCallNumber.EXIT, new int[] { 0 } ));
	
			System.out.println("Warning: added a endprogram syscall to the end of the program.");
		}

	}
	
	/**
	 * Set if the computation instructions are to be interpreted or not.
	 * By default they are.
	 * @param flag If true, computation instructions will be interpreted
	 */
	public static void setComputation(boolean flag) {
		isComputationOn = flag;
	}

	/**
	 * Test if computation instructions are to be interpreted.
	 * @return true is computation instructions are to be interpreted
	 */
	public static boolean isComputationOn() {
		return isComputationOn;
	}
	
	/**
	 * Set if the memory access instructions are to be interpreted or not.
	 * By default they are.
	 * @param flag If true, memory access instructions will be interpreted
	 */
	public static void setMemoryAccess(boolean flag) {
		isMemoryAccessOn = flag;
	}

	/**
	 * Test if memory access instructions are to be interpreted.
	 * @return true is memory access instructions are to be interpreted
	 */
	public static boolean isMemoryAccessOn() {
		return isMemoryAccessOn;
	}	

	/**
	 * Iterator over the program's list of instructions
	 * @return The iterator
	 */
	public InstructionListIterator iterator() {
		return this.instructionList.iterator();
	}

	/**
	 * Size of the program in number of instructions
	 * @return The program's number of instructions
	 */
	public int size() {
		return this.instructionList.size();
	}

	/**
	 * Get the name of the file storing the program
	 * @return
	 */
	public String getFileName() {
		return fileName;
	}
	
}
