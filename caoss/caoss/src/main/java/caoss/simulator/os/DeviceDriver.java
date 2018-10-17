package caoss.simulator.os;

import caoss.simulator.configuration.Hardware;
import caoss.simulator.hardware.Device;
import caoss.simulator.hardware.DeviceId;

public abstract class DeviceDriver {
	
	protected final Device device;
	
	protected DeviceDriver(DeviceId deviceId) {
		this.device = Hardware.devices.get(deviceId);
	}
	
	public abstract File open() throws Exception;
	
	public abstract void read(File file, long nbytes, long offset) throws Exception;
	
	public abstract void write(File file, long nbytes, long offset) throws Exception;
	
	public abstract void close(File file) throws Exception;

	
}
