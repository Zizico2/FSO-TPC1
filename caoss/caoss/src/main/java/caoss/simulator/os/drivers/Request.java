package caoss.simulator.os.drivers;

import caoss.simulator.os.ProcessControlBlock;

public class Request {

	public final ProcessControlBlock<?> pcb;
	
	public Request(ProcessControlBlock<?> pcb) {
		this.pcb = pcb;
	}
}
