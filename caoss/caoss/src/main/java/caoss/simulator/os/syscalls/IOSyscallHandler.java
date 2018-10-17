package caoss.simulator.os.syscalls;

import caoss.simulator.hardware.CPUCore;
import caoss.simulator.os.IORequestManager;

/**
 * A dummy system call handler that does nothing
 * 
 * @author Herve Paulino
 *
 */
public class IOSyscallHandler implements SysCallHandler {

	
	public synchronized void handle(CPUCore core) throws Exception {
		IORequestManager.ioRequest(core);
	}

}
