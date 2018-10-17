package caoss.simulator.os.interrupts;

import caoss.simulator.hardware.CPUCore;
import caoss.simulator.os.InterruptHandler;
import caoss.simulator.os.Logger;
import caoss.simulator.os.syscalls.SysCallNumber;
import caoss.simulator.os.syscalls.SysCallTable;

/**
 * The interrupt handler for system calls
 * 
 * @author Herve Paulino
 *
 */
public class SysCallInterruptHandler implements InterruptHandler {

	/**
	 * The handler implementation.
	 * It inspects {@link SysCallTable} to invoke the handler associated to the system call
	 * number retrieved from register(0)
	 */
	@Override
	public void handle(CPUCore cpuCore) throws Exception {
		SysCallNumber number =  cpuCore.<SysCallNumber>getRegister(0);
		Logger.fine("CPU core " + cpuCore + " is handling system call " + number);
			
		SysCallTable.getHandler(number).handle(cpuCore);
	}

}
