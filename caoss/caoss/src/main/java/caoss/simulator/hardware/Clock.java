package caoss.simulator.hardware;

import caoss.simulator.configuration.Hardware;

/**
 * Class that synchronizes the executor of the hardware 
 * 
 * @author Herve Paulino
 *
 */
public class Clock extends Thread {

	private long time = 0;
	
	private volatile boolean running = true;
	
	/**
	 * Forever invoke method {@link Clockable#tick() tick()} upon all CPU cores and 
	 * hardware devices configure in the {@link caoss.simulator.Configuration Configuration} class
	 */
	@Override
	public void run() {
		while (true) {
			
			synchronized (this) {
			while (!this.running) {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}		
			}
			}
			for (CPUCore cpuCore : Hardware.cpuCores)
				cpuCore.tick();
			
			Hardware.ram.tick();

			Hardware.devices.get(DeviceId.DUMMY).tick();
			
			Hardware.devices.get(DeviceId.TIMER).tick();
			
			this.time++;
			
			
		}
	}

	public synchronized void pause() {
		this.running = false;
	}
	
	public synchronized void resume(long value) {
		System.out.println("VALLL " + value);
		
		this.time = value;
		this.running = true;
		notifyAll();
	}
	
	public synchronized void resumeAsWas() {
		this.running = true;
		notifyAll();
	}
		
	public synchronized long getTime() {
		return this.time;
	}

}
