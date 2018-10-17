package caoss.simulator.console;

import java.util.List;

import caoss.simulator.os.OperatingSystem;

/**
 * Implementation of the "shutdown" command
 * 
 * @author Herve Paulino
 *
 */
public class ShutdownCommand extends ConsoleCommand {

	/**
	 * Shuts down the simulator
	 */
	@Override
	public void run(List<String> arguments) {
		Console.out.println("CAOS simulator shutting down...");
		while (OperatingSystem.OS.getNumberRunningProcess() > 0) 
			Thread.yield();
		System.exit(0);
	}

	@Override
	public String getCommandName() {
		return "shutdown";
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
		return "shutdown\t\t - terminate the execution of the OS (and of the simulator)";
	}

}
