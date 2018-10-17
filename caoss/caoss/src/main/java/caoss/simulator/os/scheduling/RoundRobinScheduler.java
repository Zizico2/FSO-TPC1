package caoss.simulator.os.scheduling;

import static caoss.simulator.os.OperatingSystem.OS;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import caoss.simulator.Program;
import caoss.simulator.configuration.Hardware;
import caoss.simulator.hardware.Device;
import caoss.simulator.hardware.DeviceId;
import caoss.simulator.os.Dispatcher;
import caoss.simulator.os.Logger;
import caoss.simulator.os.ProcessControlBlock;
import caoss.simulator.os.Scheduler;


public class RoundRobinScheduler implements Scheduler<SchedulingState> {
	
	private static final int QUANTUM = 10;
	
	private final Device timer = Hardware.devices.get(DeviceId.TIMER);
	
	protected final Queue<ProcessControlBlock<SchedulingState>> readyQueue = 
			new LinkedBlockingQueue<ProcessControlBlock<SchedulingState>>();
	
	
	@Override
	public synchronized void newProcess(Program program) {
		ProcessControlBlock<SchedulingState> pcb = 
				new ProcessControlBlock<SchedulingState>(program, new SchedulingState(0, 0));
		Logger.info("Create process " + pcb.pid + " to run program " + program.getFileName());
		
		if (OS.getRunningProcess() == null) 
			dispatch(pcb);
		else 
			this.readyQueue.add(pcb);

		logQueue();
	}
	
	@Override
	public synchronized void ioRequest(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": IO request");
		dispatch(readyQueue.poll());
		logQueue();
	}
	
	@Override
	public synchronized void ioConcluded(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": IO concluded");
		
		if (OS.getRunningProcess() == null) 
			dispatch(pcb);
		else
			readyQueue.add(pcb);
		
		logQueue();
	}
	
	@Override
	public synchronized void quantumExpired(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": quantum expired");
		
		this.readyQueue.add(pcb);
		dispatch(this.readyQueue.poll());
		
		logQueue();
	}

	@Override
	public synchronized void processConcluded(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": execution concluded");
		
		dispatch(readyQueue.poll());
		long turnarround = Hardware.clock.getTime() - pcb.arrivalTime;
		Logger.info("Process " + pcb.pid + " - turnarround time: " + turnarround);
		
		logQueue();
	}
	

	// Private
	
	private void dispatch(ProcessControlBlock<SchedulingState> pcb) {
		Dispatcher.dispatch(pcb);
		if (pcb != null) {			
			Logger.info("Run process " + pcb.pid + " for " + QUANTUM + " ticks" );
			timer.set(QUANTUM);
		}
		else
			timer.set(0);
	}

	private void logQueue() {
		Logger.info("Queue: " + this.readyQueue);
	}
}
