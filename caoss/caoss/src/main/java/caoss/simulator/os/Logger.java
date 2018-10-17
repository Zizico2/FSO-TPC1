package caoss.simulator.os;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * A simple logging class
 * 
 * @author Herve Paulino
 *
 */
public class Logger {

	/**
	 * The logger
	 */
	static final java.util.logging.Logger LOGGER = 
			java.util.logging.Logger.getAnonymousLogger();
	
	static {
		LOGGER.setUseParentHandlers(false);
	    Handler handler = new ConsoleHandler();
	    handler.setFormatter(new Formatter() {
	      public String format(LogRecord record) {	  
	    	return record.getMessage() + "\n";
	      }
	    });
	    handler.setLevel(Level.ALL);
	    LOGGER.addHandler(handler);
	    
	    LOGGER.setLevel(Level.INFO);
    }
	
    /**
     * Set the level of logging
     * @param level The level
     */
	public static void setLevel(Level level) {
		LOGGER.setLevel(level);
	}
	
	/**
	 * Emit an fine message
	 * @param message The message
	 */
	public static synchronized void fine(String message) {
		LOGGER.fine(message);
	}
	
	/**
	 * Emit an info message
	 * @param message The message
	 */
	public static synchronized void info(String message) {
		LOGGER.info(message);
	}
	
	/**
	 * Emit an error message
	 * @param message The message
	 */
	public static synchronized void error(String message) {
		LOGGER.severe("Error:" + message);
	}
	
	/**
	 * Emit an internal error message, ie an error resulting from a bug in the OS implementation)
	 * @param message The message
	 */
	public static synchronized void internalError(String message) {
		LOGGER.severe("Internal error: " + message);
	}
}
