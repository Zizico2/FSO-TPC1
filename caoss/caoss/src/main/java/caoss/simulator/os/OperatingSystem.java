package caoss.simulator.os;

import java.util.HashMap;
import java.util.Map;

import caoss.simulator.CAOSS;
import caoss.simulator.configuration.Hardware;
import caoss.simulator.hardware.CPUCore;
import caoss.simulator.hardware.DeviceId;

/**
 * Base class for the operating system implementation.
 * The class implements the singleton pattern.
 * 
 * @author Herve Paulino
 *
 */
public abstract class OperatingSystem {
	
	/**
	 * The single instance of the operating system
	 */
	public static final OperatingSystem OS;
	
	private volatile int numberRunningProcesses = 0;
	
	static {
		OperatingSystem localinstance = null;
		try {
			 localinstance = CAOSS.operatingSystemClass.getConstructor().newInstance();
		}
		catch (Exception e) {
			Logger.internalError("The operating system class may not have been properly "
					+ "defined in the configuration.OperatingSystem class - got error: " + e.getLocalizedMessage());
			System.exit(1);
		}
	
		OS = localinstance;
	}
	
	@SuppressWarnings("rawtypes")
	private final Map<CPUCore, ProcessControlBlock> running = 
			new HashMap<CPUCore, ProcessControlBlock>();
	
	
	/**
	 * Obtain the instance of the operating system
	 * @return The operating system
	 */
	public static final synchronized OperatingSystem getInstance() {
		return OS; 
	}
	
	/**
	 * Load the operating system
	 */
	public abstract void load();
	
	/**
	 * Retrieve the interrupt vector
	 * @return The interrupt vector
	 */
	public  abstract InterruptHandler[] getInterruptVector();
	
	/**
	 * Returns the Operating System's scheduler
	 * @return The scheduler
	 */ 
	@SuppressWarnings("rawtypes")
	public abstract Scheduler getScheduler();
	
	/**
	 * Returns the driver of the device with the given identifier
	 * @param deviceId
	 * @return
	 */
	public abstract DeviceDriver getDriver(DeviceId deviceId);

	/**
	 * Returns the Control Block of the process running in the given cpu core
	 * @param cpuCore The cpu core
	 * @return the Process Control Block
	 */
	@SuppressWarnings("rawtypes")
	public ProcessControlBlock getRunningProcess(CPUCore cpuCore) {
		return this.running.get(cpuCore);
	}

	/**
	 * Returns the Control Block of the process running in core 0
	 * @return the Process Control Block
	 */
	public Object getRunningProcess() {
		return getRunningProcess(Hardware.cpuCores[0]);
	}
	
	/**
	 * Set the info that process with the given control block is running 
	 * on CPU core \"cpuCore\"
	 * @param cpuCore The CPU core
	 * @param pcb The Control Block of the process running in cpuCore
	 */
	public <S> void setRunningProcess(CPUCore cpuCore, ProcessControlBlock<S> pcb) {
		this.running.put(cpuCore, pcb);
	}
	
	public int getNumberRunningProcess() {
		return this.numberRunningProcesses;
	}

	public void increaseNumberProcesses() {
		numberRunningProcesses++;
	}
	
	public void decreaseNumberProcesses() {
		numberRunningProcesses--;
	}

}
