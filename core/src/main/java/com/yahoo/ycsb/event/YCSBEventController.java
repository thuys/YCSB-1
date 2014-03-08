package com.yahoo.ycsb.event;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

public class YCSBEventController {
	private YCSBEventFileParser parser;
	private Set<YCSBEvent> eventSet;
	private ScheduledThreadPoolExecutor executor;
	
	public YCSBEventController(String file){
		parser = new YCSBEventFileParser(file);
		eventSet = parser.parse();
		executor = new ScheduledThreadPoolExecutor(eventSet.size());
		executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
	}
	
	public void start(){
		for(YCSBEvent event : eventSet){
			executor.schedule(event, event.getStartExecutingInMS(), TimeUnit.MILLISECONDS);
		}
		executor.prestartAllCoreThreads();
	}
	
	public void log(MeasurementsExporter exporter) throws IOException{
		for(YCSBEvent event: eventSet){
			event.log(exporter);
		}
	}
	
	public void stopStartingNewTasks(){
		executor.shutdown();
	}
	public void end(long timeout, TimeUnit timeUnit){
		try {
			executor.awaitTermination(timeout, timeUnit);
		} catch (InterruptedException e) {
		}
		executor.shutdownNow();
	}
	
	public static void main(String[] args) {
		String file = "D:/Schooljaar 2013-2014/Thesis/YCSB/test.xml";
		YCSBEventController parser = new YCSBEventController(file);
		parser.start();
		try {
		    Thread.sleep(10000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		parser.end(2000, TimeUnit.MILLISECONDS);
	}

	public void waitTillAllEventsAreFinished() {
		boolean isDone = false;
		do{
			for(YCSBEvent event : eventSet){
				isDone &= event.isExecuted();
			}
			try {
			    Thread.sleep(100);
			} catch(InterruptedException ex) {}
		}while(!isDone);
	}
	
}
