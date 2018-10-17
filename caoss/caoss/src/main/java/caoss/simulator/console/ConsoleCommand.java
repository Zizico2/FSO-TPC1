package caoss.simulator.console;

import java.util.List;

/**
 * Base class for the implementation of console commands
 * 
 * @author Herve Paulino
 *
 */
public abstract class ConsoleCommand {
	
	/**
	 * The name that the user must use to invoke the command
	 * @return The name of the command
	 */
	public abstract String getCommandName();
	
	/**
	 * The implementation of the command
	 * @param arguments List of arguments that the command receives
	 */
	public abstract void run(List<String> arguments);
	
	/**
	 * Obtain the command's number of mandatory arguments
	 * @return The number of arguments
	 */
	public abstract int getNumberArguments();
	
	/**
	 * Obtain the command's number of optional arguments
	 * @return The number of arguments
	 */
	public abstract int getNumberOptionalArguments();
	
	/**
	 * A string representation of the command's usage.
	 * Posted by the console's {@link HelpCommand help} command.
	 * @return The string representation of the command's usage.
	 */
	public abstract String usage();
	
	/**
	 * Checks if the given number of arguments abides to the command's specification
	 * @param numberOfArguments Number of arguments to check
	 * @return true if the number of arguments is valid, false otherwise
	 */
	public boolean checkNumberArguments(int numberOfArguments) {
		int mandatoryArguments = getNumberArguments();
		return ! (numberOfArguments < mandatoryArguments ||
				numberOfArguments > mandatoryArguments + getNumberOptionalArguments());
	}
}
