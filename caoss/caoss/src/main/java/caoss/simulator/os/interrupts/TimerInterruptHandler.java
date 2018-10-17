package caoss.simulator.os.interrupts;

import static caoss.simulator.os.OperatingSystem.OS;

import caoss.simulator.hardware.CPUCore;
import caoss.simulator.os.InterruptHandler;
import caoss.simulator.os.Logger;
import caoss.simulator.os.ProcessControlBlock;



public class TimerInterruptHandler implements InterruptHandler {

	@SuppressWarnings("unchecked")
	@Override
	public void  handle(CPUCore core) throws Exception {
		ProcessControlBlock<?> pcb = OS.getRunningProcess(core);
		if (pcb == null)
			Logger.internalError("Preempted the idle process");
		else
			OS.getScheduler().quantumExpired(pcb);
	}
}
