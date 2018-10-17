package caoss.simulator.configuration;

import caoss.simulator.instructions.Computation;
import caoss.simulator.instructions.MemoryLoad;
import caoss.simulator.instructions.MemoryStore;
import caoss.simulator.instructions.SysCall;

public class InstructionSet {


	/**
	 *  The class implementing the {@link caoss.simulator.instructions.Computation Computation} instruction
	 */
	public static final Class<Computation> computationClass = Computation.class;

	/**
	 *  The class implementing the {@link caoss.simulator.instructions.SysCall SysCall} instruction
	 */
	public static final Class<SysCall> sysCallClass = SysCall.class;

	/**
	 *  The class implementing the {@link caoss.simulator.instructions.MemoryLoad MemoryLoad} instruction
	 */
	public static final Class<MemoryLoad> memoryLoadAccessClass = MemoryLoad.class;

	/**
	 *  The class implementing the {@link caoss.simulator.instructions.MemoryStore MemoryStore} instruction
	 */
	public static final Class<MemoryStore> memoryStoreAccessClass = MemoryStore.class;

}
