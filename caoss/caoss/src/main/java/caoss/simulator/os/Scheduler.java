package caoss.simulator.os;

import caoss.simulator.Program;

public interface Scheduler<S> {

	/**
	 * Create a new process to run the program with given name
	 * @param program The program to run
	 */
	void newProcess(Program program);
	
	/**
	 * Process with the given control block has performed an input/output operation
	 * @param pcb The process control block
	 */
	void ioRequest(ProcessControlBlock<S> pcb);

	/**
	 * Process with the given control block has concluded the requested input/output operation
	 * @param pcb The process control block
	 */
	void ioConcluded(ProcessControlBlock<S> pcb);
	
	/**
	 * Process with the given control block has expired its quantum
	 * @param pcb The process control block
	 */
	void quantumExpired(ProcessControlBlock<S> pcb);
	
	/**
	 * Process with the given control block has concluded
	 * @param pcb The process control block
	 */
	void processConcluded(ProcessControlBlock<S> pcb);

}
