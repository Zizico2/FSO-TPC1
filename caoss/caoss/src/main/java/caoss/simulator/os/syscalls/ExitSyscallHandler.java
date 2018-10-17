package caoss.simulator.os.syscalls;

import static caoss.simulator.os.OperatingSystem.OS;

import caoss.simulator.hardware.CPUCore;
/**
 * The exit system call handler that exits
 * 
 * @author Herve Paulino
 *
 */
public class ExitSyscallHandler implements SysCallHandler {


	@SuppressWarnings("unchecked")
	@Override
	public void handle(CPUCore core) throws Exception {
		OS.getScheduler().processConcluded(OS.getRunningProcess(core));
		OS.decreaseNumberProcesses();
	}

}
