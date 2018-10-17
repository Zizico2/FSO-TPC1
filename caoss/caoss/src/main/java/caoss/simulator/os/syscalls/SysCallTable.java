package caoss.simulator.os.syscalls;

import java.util.HashMap;
import java.util.Map;

/**
 * The system call table
 * 
 * @author Herve Paulino
 *
 */
public class SysCallTable {
	
	/**
	 * The table
	 */
	static final Map<SysCallNumber, SysCallHandler> table = 
			new HashMap<SysCallNumber, SysCallHandler>();
	
	// Filling the table
	static {
		table.put (SysCallNumber.LOAD_PROGRAM, new ExecSyscallHandler());
		table.put (SysCallNumber.IO, new IOSyscallHandler());		
		table.put (SysCallNumber.EXIT, new ExitSyscallHandler());
	}
	
	/**
	 * Obtain the handler for a given system call
	 * @param number The system call number
	 * @return The handler
	 */
	public static SysCallHandler getHandler(SysCallNumber number) {
		return table.get(number);
	}
}
