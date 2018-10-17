package caoss.simulator.os;

import caoss.simulator.Program;
import caoss.simulator.configuration.Hardware;

public class ProcessControlBlock<S> {
	
	public enum State {
		NEW,
		READY,
		RUNNING,
		SUSPENDED,
		TERMINATED
	}

	private static class IdentifierFactory {
		
		private static long identifier = 0;
		
		static synchronized long newIdentifier() {
			return identifier++;
		}
	}
	
	/**
	 * The process' identifier
	 */
	public final long pid;
	
	/**
	 * The process's running context
	 */
	public final Context context;
	
	private State state;
	
	private S schedulingState;
	
	public final long arrivalTime;
	
	/**
	 * Constructor
	 * @param program Program to run
	 */
	public ProcessControlBlock(Program program, S schedulingState) {
		this.pid = IdentifierFactory.newIdentifier();
		this.context = new Context(program);
		this.arrivalTime = Hardware.clock.getTime();
		this.state = State.NEW;
		this.schedulingState = schedulingState;
	}
	
	
	public long getPid() {
		return this.pid;
	}

	public Context getContext() {
		return this.context;
	}

	public State getState() {
		return this.state;
	}


	public void setState(State state) {
		this.state = state;
	}


	public S getSchedulingState() {
		return this.schedulingState;
	}


	public long getArrivalTime() {
		return this.arrivalTime;
	}
	
	public String toString() {
		return "" + this.pid ;
	}
}
