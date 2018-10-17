package caoss.simulator.console;

import java.util.List;

import caoss.simulator.configuration.Hardware;
import caoss.simulator.hardware.Interrupt;
import caoss.simulator.os.OperatingSystem;
import caoss.simulator.os.syscalls.SysCallNumber;

/**
 * Implementation of the "exec" command
 * 
 * @author Herve Paulino
 *
 */
public class ExecCommand extends ConsoleCommand {

	/**
	 * Executes the program given as argument
	 */
	@Override
	public void run(List<String> arguments) {
		try {
			// For simplicity's sake we are assuming that the console is 
			// always run by CPU core 0, and thus the exec syscall commands 
			// are always targeted at that same core
			
			Hardware.clock.pause();
			for (String programFile: arguments) {
				Hardware.cpuCores[0].setRegisters(0, SysCallNumber.LOAD_PROGRAM, false, programFile);
				Hardware.cpuCores[0].handleInterrupt(Interrupt.SYSCALL);
			}
			Hardware.clock.resumeAsWas();
			
			//TODO: implement wait
			while (OperatingSystem.OS.getNumberRunningProcess() > 0) 
				Thread.yield();
			
		} catch (Exception e) {
			Console.err.println(e.getMessage());
		}
		
		
	}

	@Override
	public String getCommandName() {
		return "exec";
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
		return "exec prog1 ... progn\t - execute programs \"prog1\" to \"progn\" ";
	}

}
