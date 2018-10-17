package caoss.simulator.instructions;


import caoss.simulator.configuration.InstructionSet;
import caoss.simulator.os.syscalls.SysCallNumber;

/**
 * Factory for the creation of instances of each instruction supported by the CPU cores
 * 
 * @author Herve Paulino
 *
 */
public class InstructionFactory {

	/**
	 * See {@link caoss.simulator.configuration.InstructionSet#computationClass}
	 */
	private static Class<Computation> computationClass = InstructionSet.computationClass;

	/**
	 * See {@link caoss.simulator.configuration.InstructionSet#sysCallClass}
	 */
	private static Class<SysCall> sysCallClass = InstructionSet.sysCallClass;

	/**
	 * See {@link caoss.simulator.configuration.InstructionSet#memoryLoadAccessClass}
	 */
	private static Class<MemoryLoad> memoryLoadAccessClass = InstructionSet.memoryLoadAccessClass;

	/**
	 * See {@link caoss.simulator.configuration.InstructionSet#memoryStoreAccessClass}
	 */
	private static Class<MemoryStore> memoryStoreAccessClass = InstructionSet.memoryStoreAccessClass;	
	
	/**
	 * Create an instance of a {@link Computation computation instruction}
	 * @param clockCycles Clock cycles the computation takes
	 * @return An instance of the {@link caoss.simulator.configuration.InstructionSet#computationClass configured computation class}
	 */
	public static Computation newComputation(int clockCycles) {
		try {
			return computationClass.getConstructor(int.class).newInstance(clockCycles);
		}
		catch (Exception e) {
			System.err.println("Internal error: " + e.getLocalizedMessage());
			System.exit(1);
			return null; // dummy return 
		}
	}
	
	/**
	 * Create an instance of a {@link SysCall syscall instruction}
	 * @param sysCall The number/identifier of the system call
	 * @param arguments The arguments of the system call
	 * @return An instance of the {@link caoss.simulator.configuration.InstructionSet#sysCallClass configured system call class}
	 * @throws NoSuchSysCallException No such system call exception
	 */
	public static SysCall newSysCall(String sysCall, int[] arguments) throws NoSuchSysCallException {
		try {
			return sysCallClass.getConstructor(SysCallNumber.class, int[].class).newInstance(SysCallNumber.fromString(sysCall), arguments);
		}
		catch (Exception e) {
			System.err.println("Internal error: " + e.getLocalizedMessage());
			throw new NoSuchSysCallException(SysCallNumber.fromString(sysCall));
		}
	}
	
	/**
	 * Create an instance of a {@link MemoryLoad load instruction}
	 * @param address The source address
	 * @return An instance of the {@link caoss.simulator.configuration.InstructionSet#memoryLoadAccessClass configured memory load class}
	 */
	public static MemoryAccess newMemoryLoad(long address) {
		try {
			return memoryLoadAccessClass.getConstructor(long.class).newInstance(address);
		}
		catch (Exception e) {
			System.err.println("Internal error: " + e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
			return null; // dummy return 
		}
	}

	/**
	 * Create an instance of a {@link MemoryStore store instruction}
	 * @param address The destination address
	 * @return An instance of the {@link caoss.simulator.configuration.InstructionSet#memoryStoreAccessClass configured memory store class}
	 */
	public static MemoryAccess newMemoryStore(long address) {
		try {
			return memoryStoreAccessClass.getConstructor(long.class).newInstance(address);
		}
		catch (Exception e) {
			System.err.println("Internal error: " + e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
			return null; // dummy return 
		}
	}
	
	/**
	 * Create an instance of a {@link Loop loop instruction}
	 * @param numIterations Number of iterations
	 * @param instructionList The body of the loop
	 * @return An instance of the {@link Loop class}
	 */
	public static Loop newLoop(int numIterations, InstructionList instructionList) {
		return new Loop(numIterations, instructionList);
	}
	
}
