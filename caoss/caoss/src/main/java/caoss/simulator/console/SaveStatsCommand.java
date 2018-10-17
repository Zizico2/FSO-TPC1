package caoss.simulator.console;

import java.util.List;


/**
 * Implementation of the "exec" command
 * 
 * @author Herve Paulino
 *
 */
public class SaveStatsCommand extends ConsoleCommand {

	/**
	 * Executes the program given as argument
	 */
	@Override
	public void run(List<String> arguments) {
		Console.out.println("Not implemented");
	}

	@Override
	public String getCommandName() {
		return "save_stats";
	}

	@Override
	public int getNumberArguments() {
		return 0;
	}

	@Override
	public int getNumberOptionalArguments() {
		return 1;
	}
	
	@Override
	public String usage() {
		return "save_stats [file]\t - write the current statistics of the SO in the console or in the given file";
	}

}
