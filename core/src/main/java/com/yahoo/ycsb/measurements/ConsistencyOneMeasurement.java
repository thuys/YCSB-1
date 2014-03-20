package com.yahoo.ycsb.measurements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistencyOneMeasurement {
	private HashMap<OperationType, TreeMap<Long, Stack<Long>>> measurementInsertMap;	
	private int threadNumber;
	
	ConsistencyOneMeasurement(int threadNumber){
		this.threadNumber = threadNumber;
		measurementInsertMap = new HashMap<OperationType, TreeMap<Long, Stack<Long>>>();
		
	}
	
	public void addMeasurement(long time, OperationType type, long delay){
		
		if(!measurementInsertMap.containsKey(type)){
			measurementInsertMap.put(type, new TreeMap<Long, Stack<Long>>());
		}
		if(!measurementInsertMap.get(type).containsKey(time)){
			measurementInsertMap.get(type).put(time, new Stack<Long>());
		}
		measurementInsertMap.get(time).get(type).add(delay);
	}
	
	public int getThreadNumber(){
		return threadNumber;
	}
	
	public Set<Long> getTimes(OperationType type){
		return new TreeSet<Long>(measurementInsertMap.get(type).keySet());
	}
	
	public boolean hasDelay(OperationType type, long time){
		return measurementInsertMap.containsKey(type) && measurementInsertMap.get(type).containsKey(time);
	}
	
	public long getLastDelay(OperationType type, long time){
		return measurementInsertMap.get(type).get(time).lastElement();
	}
	
	public Collection<Long> getAllDelays(OperationType type, long time){
		return new ArrayList<Long>(measurementInsertMap.get(type).get(time));
	}
	
	public int getNumberOfDelays(OperationType type, long time){
		if(!measurementInsertMap.containsKey(type))
			return 0;
		if(!measurementInsertMap.get(type).containsKey(time))
			return 0;
		return measurementInsertMap.get(type).get(time).size();
	}
}
