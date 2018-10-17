package caoss.simulator.console;

import java.util.List;


/**
 * Implementation of the "exec" command
 * 
 * @author Herve Paulino
 *
 */
public class ResetStatsCommand extends ConsoleCommand {

	/**
	 * Executes the program given as argument
	 */
	@Override
	public void run(List<String> arguments) {
		Console.out.println("Not implemented");
	}

	@Override
	public String getCommandName() {
		return "reset_stats";
	}
	
	@Override
	public int getNumberArguments() {
		return 0;
	}

	@Override
	public int getNumberOptionalArguments() {
		return 0;
	}

	@Override
	public String usage() {
		return "reset_stats\t\t - reset the statistics of the SO";
	}

}
