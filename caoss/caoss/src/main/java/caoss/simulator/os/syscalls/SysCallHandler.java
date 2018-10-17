package caoss.simulator.os.syscalls;

import caoss.simulator.os.InterruptHandler;

/**
 * The interface for system call handlers
 * 
 * For protection purposes, syscall handler implementations should be package private
 * 
 * @author Herve Paulino
 *
 */
public interface SysCallHandler extends InterruptHandler { }
