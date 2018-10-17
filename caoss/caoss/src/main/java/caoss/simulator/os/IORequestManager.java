package caoss.simulator.os;

import static caoss.simulator.os.OperatingSystem.OS;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import caoss.simulator.hardware.CPUCore;
import caoss.simulator.hardware.DeviceId;
import caoss.simulator.os.drivers.Request;

public class IORequestManager {

	@SuppressWarnings("rawtypes")
	private final static List<ProcessControlBlock> suspended = 
			new ArrayList<ProcessControlBlock>();

	private final static Queue<Request> requests = new LinkedBlockingQueue<Request>();
	 
	private static Request currentRequest;
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static synchronized void ioRequest(CPUCore core) throws Exception {
		ProcessControlBlock pcb = OS.getRunningProcess(core);
		suspended.add(pcb);
		OS.getScheduler().ioRequest(pcb);
		
		if (currentRequest == null) {
			currentRequest = new Request(pcb);
			ioRequest();
		}
		else 
			requests.add(new Request(pcb));	
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static synchronized void processNext(CPUCore core) throws Exception {
		if (currentRequest == null)
			return;
		
		ProcessControlBlock pcb = currentRequest.pcb;
		suspended.remove(pcb);
		OS.getScheduler().ioConcluded(pcb);
		
		currentRequest = requests.poll();
		if (currentRequest != null) 
			ioRequest();
	}
	
	public static void log() {
		Logger.info("Blocked " + suspended);
	}
	
	// Single IO device
	
	private static final DeviceDriver driver = 
			OS.getDriver(DeviceId.DUMMY);
	
	private static void ioRequest() throws Exception {
			driver.read(null, 0, 0);
	}
}
