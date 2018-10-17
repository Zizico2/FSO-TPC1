package caoss.simulator.os.syscalls;

import static caoss.simulator.os.OperatingSystem.OS;

import java.io.FileInputStream;

import caoss.simulator.hardware.CPUCore;
import caoss.simulator.parser.Parser;

class ExecSyscallHandler implements SysCallHandler {
	
	@Override
	public void handle(CPUCore cpuCore) throws Exception {
		Parser parser = new Parser(new FileInputStream(cpuCore.<String>getRegister(2)));
		OS.getScheduler().newProcess(parser.Program(cpuCore.<String>getRegister(2)));
		OS.increaseNumberProcesses();
	}
}