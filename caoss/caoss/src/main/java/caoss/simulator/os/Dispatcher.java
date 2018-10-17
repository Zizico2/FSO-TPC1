package caoss.simulator.os;

import static caoss.simulator.os.OperatingSystem.OS;

import caoss.simulator.configuration.Hardware;
import caoss.simulator.hardware.CPUCore;

public class Dispatcher {
	
	public static <S>  void dispatch(ProcessControlBlock<S> pcb, CPUCore core) {
		// ProcessControlBlock running = OS.getRunning(core);
		if (pcb != null) {
			OS.setRunningProcess(core, pcb);
			core.load(pcb.context);	
		}
		else {
			OS.setRunningProcess(core, null);
			core.load(null);	
		}
	}
	

	public static <S> void dispatch(ProcessControlBlock<S> pcb) {
		dispatch(pcb, Hardware.cpuCores[0]);
	}
}
