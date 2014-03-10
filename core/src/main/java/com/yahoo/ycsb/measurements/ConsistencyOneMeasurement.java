package com.yahoo.ycsb.measurements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistencyOneMeasurement {
	private Map<Long, Stack<Long>> measurementMap;
	
	private int threadNumber;
	
	public ConsistencyOneMeasurement(int threadNumber){
		this.threadNumber = threadNumber;
		measurementMap = new TreeMap<Long, Stack<Long>>();
	}
	
	public void addMeasurement(long time, long delay){
		if(!measurementMap.containsKey(time)){
			measurementMap.put(time, new Stack<Long>());
		}
		measurementMap.get(time).add(delay);
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
	
	public long getLastDelay(long time){
		return measurementMap.get(time).lastElement();
	}
	
	public Collection<Long> getAllDelays(long time){
		return new ArrayList<Long>(measurementMap.get(time));
	}
	
	public int getNumberOfDelays(long time){
		return measurementMap.get(time).size();
	}
}
