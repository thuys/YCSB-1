package com.yahoo.ycsb.measurements;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistencyOneMeasurement {
	private Map<Long, Long> measurementMap;
	
	private int threadNumber;
	
	public ConsistencyOneMeasurement(int threadNumber){
		this.threadNumber = threadNumber;
		measurementMap = new TreeMap<Long, Long>();
	}
	
	public void addMeasurement(long time, long delay){
		measurementMap.put(time, delay);
	}
	
	public int getThreadNumber(){
		return threadNumber;
	}
	
	public Set<Long> getTimes(){
		return new TreeSet<Long>(measurementMap.keySet());
	}
	
	public boolean hasDelay(long time){
		return measurementMap.containsKey(time);
	}
	
	public long getDelay(long time){
		return measurementMap.get(time);
	}
}
