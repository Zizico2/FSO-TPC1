package caoss.simulator.hardware;

/**
 * The interface for hardware that must be synchronized by the {@link Clock}
 * 
 * @author Herve Paulino
 *
 */
public interface Clockable {
		
	/**
	 * Method invoked by {@link Clock} in each time instance
	 */
	void tick();

	/**
	 * Textual description of the hardware
	 * @return The textual description
	 */
	String getDescription();
}
