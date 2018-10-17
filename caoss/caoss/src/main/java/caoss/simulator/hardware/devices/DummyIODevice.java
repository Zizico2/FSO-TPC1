package caoss.simulator.hardware.devices;

import caoss.simulator.hardware.Device;
import caoss.simulator.hardware.Interrupt;

public class DummyIODevice extends Device {

	private final int REQUEST_TIME = 20;
	
	private int runningTime = 0;
	
	@Override
	public void tick() {
		if (--this.runningTime == 0)
			interrupt(Interrupt.DUMMY);
	}

	@Override
	public String getDescription() {
		return "Dummy IO device";
	}

	@Override
	public void set(Object... arguments) {
		this.runningTime = REQUEST_TIME;
	}

}
