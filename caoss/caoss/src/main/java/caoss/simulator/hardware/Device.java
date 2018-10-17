package caoss.simulator.hardware;

public abstract class Device extends Interrupter implements Clockable {

	public abstract void set(Object...arguments);
}
