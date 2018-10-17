package caoss.simulator.console;

import java.util.List;

/**
 * Implementation of the "help" command
 * 
 * @author Herve Paulino
 *
 */
public class HelpCommand extends ConsoleCommand {

	/**
	 * Prints the usage of every command offered by the console
	 */
	@Override
	public void run(List<String> arguments) {
		for (ConsoleCommand command : Console.commands.values()) {
			Console.out.println(command.usage());
		}
	}

	@Override
	public String getCommandName() {
		return "help";
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
		return "help\t\t\t - information on the commands available.";
	}

}
