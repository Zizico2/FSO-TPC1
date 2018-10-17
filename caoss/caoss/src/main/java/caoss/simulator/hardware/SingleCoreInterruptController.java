package caoss.simulator.hardware;

import caoss.simulator.configuration.Hardware;

public class SingleCoreInterruptController extends AbstractInterruptController {

	@Override
	protected CPUCore selectCPUCore() {
		return Hardware.cpuCores[0];
	}

}
