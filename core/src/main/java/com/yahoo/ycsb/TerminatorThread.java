/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.
 */
package com.yahoo.ycsb;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.event.YCSBEventController;

/**
 * A thread that waits for the maximum specified time and then interrupts all the client
 * threads passed as the Vector at initialization of this thread.
 * 
 * The maximum execution time passed is assumed to be in seconds.
 * 
 * @author sudipto
 *
 */
public class TerminatorThread extends Thread {
  
  private Vector<Thread> threads;
  private long maxExecutionTime;
  private List<Workload> workloads;
  private long waitTimeOutInMS;
  private YCSBEventController eventController;
  
  public TerminatorThread(long maxExecutionTime, Vector<Thread> threads, 
		  List<Workload> workloads, YCSBEventController eventController) {
    this.maxExecutionTime = maxExecutionTime;
    this.threads = threads;
    this.workloads = workloads;
    waitTimeOutInMS = 2000;
    this.eventController =  eventController;
    System.err.println("Maximum execution time specified as: " + maxExecutionTime + " secs");
  }
  
  public void run() {
	long startTime = System.currentTimeMillis();
	long now = System.currentTimeMillis();
	long timeToSleep = startTime-now + maxExecutionTime*1000;
	while(timeToSleep>0){
	    try {
	    	now = System.currentTimeMillis();
	    	timeToSleep = startTime-now + maxExecutionTime*1000;
	    	//System.err.println("TEST: " +timeToSleep);
	    	if(timeToSleep>0)
	    		Thread.sleep(timeToSleep);
	    } catch (InterruptedException e) {
	      System.err.println("Could not wait until max specified time, TerminatorThread interrupted:" +timeToSleep);
	      e.printStackTrace();
	      
	    }
	}
    System.err.println("Maximum time elapsed. Requesting stop for the workload.");
    this.requestStopForWorkloads();
    if(eventController != null){
    	eventController.stopStartingNewTasks();
    }
    System.err.println("Stop requested for workload. Now Joining!");
    for (Thread t : threads) {
      while (t.isAlive()) {
        try {
          t.join(waitTimeOutInMS);
          if (t.isAlive()) {
            System.err.println("Still waiting for thread " + t.getName() + " to complete.");
          }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for thread " + t.getName() + " to complete.");
          // Do nothing. Don't know why I was interrupted.
        }
      }
    }
    if(eventController != null){
    	eventController.end(waitTimeOutInMS, TimeUnit.MILLISECONDS);
    }
  }
  
  private void requestStopForWorkloads(){
	  for(Workload currentWorkload: workloads){
		  currentWorkload.requestStop();
	  }
  }
  
}
