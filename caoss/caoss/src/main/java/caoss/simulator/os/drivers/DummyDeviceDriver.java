package caoss.simulator.os.drivers;

import caoss.simulator.hardware.DeviceId;
import caoss.simulator.os.DeviceDriver;
import caoss.simulator.os.File;

public class DummyDeviceDriver extends DeviceDriver {
	

	public DummyDeviceDriver(DeviceId deviceId) {
		super(deviceId);
	}
	
	@Override
	public File open() throws Exception {
		return null;
	}

	@Override
	public void read(File file, long nbytes, long offset) throws Exception {
		device.set();
	}

	@Override
	public void write(File file, long nbytes, long offset) throws Exception {
		device.set();
	}

	@Override
	public void close(File file) throws Exception {
	}

	
}
