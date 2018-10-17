package caoss.simulator.os.scheduling;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import caoss.simulator.Program;
import caoss.simulator.configuration.Hardware;
import caoss.simulator.hardware.DeviceId;
import caoss.simulator.hardware.Timer;
import caoss.simulator.os.Dispatcher;
import caoss.simulator.os.Logger;
import caoss.simulator.os.ProcessControlBlock;
import caoss.simulator.os.Scheduler;

import static caoss.simulator.os.ProcessControlBlock.State.*;


public class FSOScheduler implements Scheduler<SchedulingState> {

	private static final int QUANTUM = 10;
	private static final int QUOTA = 20;


	private final Queue<ProcessControlBlock<SchedulingState>>[] readyQueues;
	private final Queue<ProcessControlBlock<SchedulingState>> blockedQueue;
	private ProcessControlBlock<SchedulingState> running; // just one CPU

	private final Timer timer = (Timer) Hardware.devices.get(DeviceId.TIMER);


	@SuppressWarnings("unchecked")
	public FSOScheduler( ) {
		this.readyQueues = (Queue<ProcessControlBlock<SchedulingState>>[]) new LinkedBlockingQueue[2];
		this.blockedQueue = new LinkedBlockingQueue<ProcessControlBlock<SchedulingState>>();

		for (int i = 0; i < 2; i++)
			this.readyQueues[i] = new LinkedBlockingQueue<ProcessControlBlock<SchedulingState>>();

	}


	@Override
	public synchronized void newProcess(Program program) {
		ProcessControlBlock<SchedulingState> pcb =
				new ProcessControlBlock<SchedulingState>(program,
						new SchedulingState(0, QUOTA));
		Logger.info("Create process " + pcb.pid + " to run program " + program.getFileName());
		if(running != null && running.getState() == RUNNING) {
			pcb.setState(READY);
			readyQueues[pcb.getSchedulingState().getLevel()].add(pcb);
		}
		else{
			pcb.setState(RUNNING);
			dispatch(pcb, QUANTUM);
		}
		logQueues();
	}

	@Override
	public synchronized void ioRequest(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": IO request");
		pcb.setState(SUSPENDED);
		blockedQueue.add(pcb);
		chooseNext();
		logQueues();
	}


	@Override
	public synchronized void ioConcluded(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": IO concluded");
		pcb.getSchedulingState().setQuota(QUOTA);
		pcb.getSchedulingState().setLevel(0);
		blockedQueue.remove();
		if(running != null && running.getState() == RUNNING) {
			pcb.setState(READY);
			readyQueues[0].add(pcb);
		}
		else{
			pcb.setState(RUNNING);
			dispatch(pcb, QUANTUM);
		}

		logQueues();
	}

	@Override
	public synchronized void quantumExpired(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": quantum expired");
		int quota = pcb.getSchedulingState().getQuota() - (pcb.getSchedulingState().getLevel()+1)*QUANTUM;
		if (quota <= 0) {
			pcb.getSchedulingState().setQuota(QUOTA);
			if (pcb.getSchedulingState().getLevel() == 0){
				pcb.getSchedulingState().setLevel(1);
				Logger.info("Process " + pcb.pid + ": quota expired");
			}
		} else
			pcb.getSchedulingState().setQuota(quota);
		pcb.setState(READY);
		readyQueues[pcb.getSchedulingState().getLevel()].add(pcb);
		chooseNext();
		logQueues();
	}


	@Override
	public synchronized void processConcluded(ProcessControlBlock<SchedulingState> pcb) {
		Logger.info("Process " + pcb.pid + ": execution concluded");
		long turnarround = Hardware.clock.getTime() - pcb.arrivalTime;
		Logger.info("Process " + pcb.pid + ": turnarround time: " + turnarround);
		pcb.setState(TERMINATED);
		chooseNext();
		logQueues();
	}



	private void chooseNext() {
		ProcessControlBlock<SchedulingState> pcb;
		for (Queue<ProcessControlBlock<SchedulingState>> queue : this.readyQueues) {
			pcb = queue.poll();
			if (pcb != null){
				pcb.setState(RUNNING);
				dispatch(pcb, (pcb.getSchedulingState().getLevel()+1)*QUANTUM);
				return;
			}
		}
		dispatch(null,0);
	}

	private void dispatch(ProcessControlBlock<SchedulingState> pcb, int quantum) {
		Dispatcher.dispatch(pcb);
		running = pcb;
		if (pcb != null) {
			SchedulingState state = pcb.getSchedulingState();
			state.setSchedulingTime(Hardware.clock.getTime());
			timer.set(quantum);
			Logger.info("Run process "+ pcb.pid +" (quantum="+ quantum +", quota="+ state.getQuota()+")");
		}
		else
			timer.set(0);
	}


	private void logQueues() {
		int i = 0;
		for (Queue<ProcessControlBlock<SchedulingState>> queue : this.readyQueues) {
			Logger.info("Queue " + i + ": " + queue);
			i++;
		}
		Logger.info("Blocked " + blockedQueue);
	}
}
