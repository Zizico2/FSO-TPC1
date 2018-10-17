package caoss.simulator.os;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import caoss.simulator.configuration.Hardware;
import caoss.simulator.console.Console;
import caoss.simulator.hardware.Device;
import caoss.simulator.hardware.DeviceId;
import caoss.simulator.hardware.Interrupt;
import caoss.simulator.os.drivers.DummyDeviceDriver;
import caoss.simulator.os.interrupts.DummyDeviceInterruptHandler;
import caoss.simulator.os.interrupts.SysCallInterruptHandler;
import caoss.simulator.os.interrupts.TimerInterruptHandler;
import caoss.simulator.os.scheduling.FSOScheduler;


/**
 * The FSO Operating System class
 * 
 * @author Herve Paulino
 *
 */
public class FSOOS extends OperatingSystem {

	/**
	 * The scheduler
	 */
	private final Scheduler<?> scheduler = new FSOScheduler();

	/**
	 * The interrupt vector
	 */
	private final InterruptHandler[] interruptVector;

	/**
	 * The device driver map: projects device ids into device drive implementations
	 */
	private final Map<DeviceId, DeviceDriver> drivers = 
			new HashMap<DeviceId, DeviceDriver>();
	
	public FSOOS() {
		interruptVector = new InterruptHandler[Interrupt.size()];
		interruptVector[Interrupt.SYSCALL.ordinal()] = new SysCallInterruptHandler();
		interruptVector[Interrupt.TIMER.ordinal()] = new TimerInterruptHandler();
		interruptVector[Interrupt.DUMMY.ordinal()] = new DummyDeviceInterruptHandler();
		
		drivers.put(DeviceId.DUMMY, new DummyDeviceDriver(DeviceId.DUMMY));
	}
	
	
	@Override
	public void load() {
		Console.out.println("Loading the FSO scheduling simulator");
		Console.out.println("Available hardware:");
		for (Entry<DeviceId, Device> deviceEntry : Hardware.devices.entrySet()) {
			Console.out.println("   " + deviceEntry.getValue().getDescription() + 
					" (with id " + deviceEntry.getKey() + ")");
		}
		Console.out.println();
		new Console("fso-os").run();
	}

	@Override
	public InterruptHandler[] getInterruptVector() {
		return interruptVector;
	}
	
	@Override
	public Scheduler<?> getScheduler() {
		return scheduler;
	}
	
	@Override
	public DeviceDriver getDriver(DeviceId deviceId) {
		return drivers.get(deviceId);
	}

}
