package caoss.simulator.configuration;

import java.util.HashMap;
import java.util.Map;

import caoss.simulator.hardware.AbstractInterruptController;
import caoss.simulator.hardware.CPUCore;
import caoss.simulator.hardware.Clock;
import caoss.simulator.hardware.Device;
import caoss.simulator.hardware.DeviceId;
import caoss.simulator.hardware.RAM;
import caoss.simulator.hardware.SimpleCPUCore;
import caoss.simulator.hardware.SingleCoreInterruptController;
import caoss.simulator.hardware.Timer;
import caoss.simulator.hardware.devices.DummyIODevice;

public class Hardware {
	
	// Units
	
	public static final int KByte = 1024;
	public static final int MByte = 1024 * KByte;
	public static final int GByte = 1024 * MByte;
	
	// Hardware 
	
	public static final Clock  clock = new Clock();
	
	
	/**
	 * The class implementing the behavior of a CPU core
	 */
	public static final Class<? extends CPUCore> cpuCoreClass = SimpleCPUCore.class;

	/**
	 * Number of cores of the simulated CPU
	 */
	public static final int numberOfCPUCores = 1;
	
	
	/**
	 * The array of @see numberOfCPUCores CPU cores
	 */
	public static final CPUCore[] cpuCores = new CPUCore[numberOfCPUCores];
	
	static {
		for (int i = 0; i < numberOfCPUCores; i++) {
			try {
				cpuCores[i] = cpuCoreClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	};
	
	/**
	 * Installed RAM
	 */
	public static final RAM ram = new RAM(20 * MByte, 32);
	
	/**
	 *  The class implementing the interrupt controller 
	 */
	public static final Class<? extends AbstractInterruptController> interruptControlerClass = 
			SingleCoreInterruptController.class;
	
	/**
	 * The computer's set of devices
	 */
	@SuppressWarnings("serial")
	public static final Map<DeviceId, Device> devices =
			new HashMap<DeviceId, Device> () {{
				put (DeviceId.TIMER, new Timer());
				put (DeviceId.DUMMY, new DummyIODevice());
			// Fill with other hardware components
			}};
	


}
