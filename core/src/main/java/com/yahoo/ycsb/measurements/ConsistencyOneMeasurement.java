package com.yahoo.ycsb.measurements;

import java.util.HashMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;

import com.yahoo.ycsb.util.Pair;

public class ConsistencyOneMeasurement {
	private HashMap<OperationType, TreeMap<Long, Stack<Pair<Long, Long>>>> measurementInsertMap;	
	private int threadNumber;
	
	ConsistencyOneMeasurement(int threadNumber){
		this.threadNumber = threadNumber;
		measurementInsertMap = new HashMap<OperationType, TreeMap<Long, Stack<Pair<Long, Long>>>>();
		
	}
	
	public void addMeasurement(long time, OperationType type, long start, long delay){
		
		if(!measurementInsertMap.containsKey(type)){
			measurementInsertMap.put(type, new TreeMap<Long, Stack<Pair<Long, Long>>>());
		}
		if(!measurementInsertMap.get(type).containsKey(time)){
			measurementInsertMap.get(type).put(time, new Stack<Pair<Long, Long>>());
		}
		
		measurementInsertMap.get(type).get(time).add(new Pair<Long, Long>(start, delay));
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
		return measurementInsertMap.get(type).get(time).lastElement().getY();
	}
	
//	public Collection<Long> getAllDelays(OperationType type, long time){
//		return new ArrayList<Long>(measurementInsertMap.get(type).get(time));
//	}
	
	public int getNumberOfDelays(OperationType type, long time){
		if(!measurementInsertMap.containsKey(type))
			return 0;
		if(!measurementInsertMap.get(type).containsKey(time))
			return 0;
		return measurementInsertMap.get(type).get(time).size();
	}
}
