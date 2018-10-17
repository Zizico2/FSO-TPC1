package caoss.simulator.os.interrupts;

import caoss.simulator.hardware.CPUCore;
import caoss.simulator.os.IORequestManager;
import caoss.simulator.os.InterruptHandler;

public class DummyDeviceInterruptHandler implements InterruptHandler {

	
	@Override
	public void  handle(CPUCore cpuCore) throws Exception {
		IORequestManager.processNext(cpuCore);
	}
}
