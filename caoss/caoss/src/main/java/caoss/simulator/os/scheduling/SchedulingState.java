package caoss.simulator.os.scheduling;

/**
 *  Scheduling state information to be stored in each Process Control Block
 *  Specially designed for Multi-Level Feedback Queue algorithms
 * 
 * @author Herve Paulino
 *
 */
public class SchedulingState {
	
	/**
	 * Quota or allotment currently assigned to the process
	 */
	private int quota;
	
	/**
	 * The priority level currently assigned to the process
	 */
	private int level;
	
	/**
	 * The instant when the process was last scheduled 
	 */
	private long schedulingTime;

	
	SchedulingState(int level, int quota) {
		this.level = level;
		this.quota = quota;
	}	
	
	/**
	 * Retrieve the quota (or allotment) currently assigned to the process
	 * @return The quota
	 */
	public int getQuota() {
		return quota;
	}

	/**
	 * Set the quota (or allotment) to assign to the process
	 * @param quota The quota
	 */
	public void setQuota(int quota) {
		this.quota = quota;
	}

	/**
	 * Retrieve the priority level currently assigned to the process
	 * @return The priority level
	 */
	public int getLevel() {
		return level;
	}

	/**
	 * Set the priority level assigned to the process
	 * @param level The priority level
	 */
	public void setLevel(int level) {
		this.level = level;
	}

	/**
	 * Retrieve the instant when the process was last scheduled 
	 * @return The instant
	 */
	public long getSchedulingTime() {
		return schedulingTime;
	}

	/**
	 * Set the instant when the process was last scheduled 
	 * @param time The instant
	 */
	public void setSchedulingTime(long time) {
		this.schedulingTime = time;
	}
	
}
