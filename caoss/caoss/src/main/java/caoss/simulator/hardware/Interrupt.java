package caoss.simulator.hardware;

/**
 * The enumeration of the supported interrupts
 * 
 * @author Herve Paulino
 *
 */
public enum Interrupt {

	SYSCALL,  // System call
	TIMER,
	DUMMY; 
	
	/**
	 * The number of supported interrupts
	 * @return The number of supported interrupts
	 */
	public static int size() {
		return values().length;
	}
}
