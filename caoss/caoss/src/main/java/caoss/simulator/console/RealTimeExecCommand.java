package caoss.simulator.console;

import java.util.List;

import caoss.simulator.configuration.Hardware;
import caoss.simulator.hardware.Interrupt;
import caoss.simulator.os.syscalls.SysCallNumber;

/**
 * Implementation of the "rtexec" command
 * 
 * @since April, 2015
 *
 * @author Herve Paulino
 *
 */
public class RealTimeExecCommand extends ConsoleCommand {

	/**
	 * Executes the program given as argument
	 */
	@Override
	public void run(List<String> arguments) {
		try {
			// For simplicity's sake we are assuming that the console is 
			// always run by CPU core 0, and thus the rtexec syscall commands 
			// are always targeted at that same core
			
			for (String programFile: arguments) {
				Hardware.cpuCores[0].setRegisters(0, SysCallNumber.LOAD_PROGRAM, true, programFile);
				Hardware.cpuCores[0].handleInterrupt(Interrupt.SYSCALL);
			}
			
		} catch (Exception e) {
			Console.err.println(e.getMessage());
		}
	}

	@Override
	public String getCommandName() {
		return "rtexec";
	}
	
	@Override
	public int getNumberArguments() {
		return 1;
	}

	@Override
	public int getNumberOptionalArguments() {
		return Integer.MAX_VALUE - 1;
	}

	@Override
	public String usage() {
		return "rtexec prog\t\t - execute program \"prog\" with realtime priority";
	}

}
