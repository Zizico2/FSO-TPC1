package caoss.simulator.hardware;

/**
 * Invalid register number exception.
 * 
 * @author Herve Paulino
 *
 */
public class InvalidRegisterException extends Exception {

	/**
	 * Default serial version ID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor
	 * @param register The number of the register that caused the exception
	 */
	public InvalidRegisterException(int register) {
		super("Invalid register number " + register);
	}

}
