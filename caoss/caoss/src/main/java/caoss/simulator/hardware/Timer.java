 package caoss.simulator.hardware;


public class Timer extends Device {

	private int ticks = 0;
	
	@Override
	public void tick() {
		if (ticks == 0)
			return; 

		if (--ticks == 0)
			interrupt(Interrupt.TIMER);
	}
	
	@Override
	public void set(Object... arguments) {
		this.ticks = (Integer) arguments[0];
	}
	
	@Override
	public String getDescription() {
		return "Timer";
	}

}
