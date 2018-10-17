package caoss.simulator.hardware;

public class RAM implements Clockable {
	
	/**
	 * Capacity in bytes
	 */
	public final long capacity; 
	
	/**
	 * Physical address size (in bits)
	 */
	public final int physicalAddressSize; 
	
	/**
	 * Unit of addressing (in bytes)
	 */
	public final int addressingUnit; 

	
	/**
	 * Constructor for byte-addressable memory
	 * @param capacity Capacity in bytes
	 * @param physicalAddressSize Physical address size (in bits)
	 */
	public RAM(long capacity, int physicalAddressSize) {
		this(capacity, physicalAddressSize, 1);
	}
	
	/**
	 * Constructor 
	 * @param capacity Capacity in bytes
	 * @param physicalAddressSize Physical address size (in bits)
	 * @param addressingUnit Unit of addressing (in bytes)
	 */
	public RAM(long capacity, int physicalAddressSize, int addressingUnit) {
		this.capacity = capacity;
		this.physicalAddressSize = physicalAddressSize;
		this.addressingUnit = addressingUnit;
	}
	
	@Override
	public void tick() {
		// do not implement for now
		
	}

	@Override
	public String getDescription() {
		return "RAM: " + capacity +" bytes";
	}
	
}
