package caoss.simulator.os;

import caoss.simulator.hardware.CPUCore;

/**
 * Interface for an interrupt handler
 * 
 * For protection purposes, interrupt handler implementations should be package private
 * 
 * @author Herve Paulino
 *
 */
public interface InterruptHandler {

	/**
	 * Handler implementation
	 * @param core The core where the interrupt is being handled
	 */
	void handle(CPUCore core) throws Exception;
}
