package caoss.simulator.hardware;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import caoss.simulator.configuration.Hardware;
import caoss.simulator.instructions.Instruction;
import caoss.simulator.os.Context;
import caoss.simulator.os.Logger;
import caoss.simulator.os.OperatingSystem;

public class SimpleCPUCore extends CPUCore {
	
	private static class IdentifierFactory {
		
		private static int identifier = 0;
		
		static synchronized int newIdentifier() {
			return identifier++;
		}
	}
	
	/**
	 * Number of registers included in each CPU core
	 */
	private final static int NUMBER_OF_REGISTERS = 20;
	
	/**
	 * The core's registers
	 */
	private final Object[] registers = new Object[NUMBER_OF_REGISTERS];
	
	/**
	 * CPU clocks that the instruction currently in execution will still take on the core
	 * before concluding
	 */
	private int instructionRunningTime;
	
	/**
	 * The core's identifier
	 */
	private final int identifier = IdentifierFactory.newIdentifier();

	private Queue<Interrupt> interrupts = new LinkedBlockingQueue<Interrupt>();

	
	private volatile long instant0 = 0;
	

	@Override
	public synchronized void load(Context context) {
		super.load(context);
		instructionRunningTime = 0;
	}

	/**
	 * The fetch-decode-execute loop
	 */
	@Override
	public synchronized void tick() {
		if (interrupts.size() > 0) {
			Interrupt interrupt = interrupts.poll();
			try {
				Logger.fine("CPU core " + this.identifier + " received interruption " + interrupt + 
						" at instant " + (Hardware.clock.getTime() - instant0));
	
				OperatingSystem.getInstance().getInterruptVector()[interrupt.ordinal()].handle(this);
			}
			catch (Exception e) {
					e.printStackTrace();
			}
		}
		else if (context != null) {
			
			if (instructionRunningTime == 0) {
				try {
					// Fetch
					Instruction inst = fetch();
				
					// Decode
					this.instructionRunningTime = inst.getCPUClocks();
					// Execute
					inst.run(this);
				} catch (Exception e) {
					context = null;
					// TODO send exception to the OS
					e.printStackTrace();
				}
			}
			instructionRunningTime--;
		}
	}

	@Override
	public String getDescription() {
		return "Simple CPU core " + this.identifier;
	}

	@Override
	public synchronized void handleInterrupt(Interrupt interrupt) throws Exception {
		if (instant0 == 0)
			instant0 = Hardware.clock.getTime();
		
		if (interrupt == Interrupt.SYSCALL) {
			try {
				Logger.fine("CPU core " + this.identifier + " received interruption " + interrupt + 
						" at instant " + (Hardware.clock.getTime() - instant0));
	
				OperatingSystem.getInstance().getInterruptVector()[interrupt.ordinal()].handle(this);
			}
			catch (Exception e) {
					throw e;
			}
		}
		else {
			interrupts .add(interrupt);
		}
	}

	@Override
	public void setRegisters(int register, Object... values) throws InvalidRegisterException {

		for (Object value : values) {
			if (register >= NUMBER_OF_REGISTERS)
				throw new InvalidRegisterException(register);
			this.registers[register++] = value;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getRegister(int i) throws InvalidRegisterException {
		if (i >= NUMBER_OF_REGISTERS) {
			throw new InvalidRegisterException(i);
		}
		return (T) registers[i];
	}

	@Override
	public Object[] getRegisters() {
		return this.registers;
	}

	@Override 
	public String toString() {
		return "" + this.identifier;
	}
	
}
