package com.yahoo.ycsb.measurements;

import java.util.HashMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;

import com.yahoo.ycsb.util.Pair;

public class ConsistencyOneMeasurement {
	private HashMap<OperationType, TreeMap<Long, Stack<Pair<Long, Long, Long>>>> measurementInsertMap;	
	private int threadNumber;
	
	ConsistencyOneMeasurement(int threadNumber){
		this.threadNumber = threadNumber;
		measurementInsertMap = new HashMap<OperationType, TreeMap<Long, Stack<Pair<Long, Long, Long>>>>();
		
	}
	
	public void addMeasurement(long time, OperationType type, long start, Long delay, Long value){
		
		if(!measurementInsertMap.containsKey(type)){
			measurementInsertMap.put(type, new TreeMap<Long, Stack<Pair<Long, Long, Long>>>());
		}
		if(!measurementInsertMap.get(type).containsKey(time)){
			measurementInsertMap.get(type).put(time, new Stack<Pair<Long, Long, Long>>());
		}
		
		measurementInsertMap.get(type).get(time).add(new Pair<Long, Long, Long>(start, delay, value));
	}
	
	public int getThreadNumber(){
		return threadNumber;
	}
	
	public Set<Long> getTimes(OperationType type){
		if(measurementInsertMap.containsKey(type))
			return new TreeSet<Long>(measurementInsertMap.get(type).keySet());
		
		return new TreeSet<Long>();
	}
	
	public boolean hasDelay(OperationType type, long time){
		return measurementInsertMap.containsKey(type) && measurementInsertMap.get(type).containsKey(time);
	}
	
	public Long getLastDelay(OperationType type, long time){
		Pair<Long, Long, Long> last = null;
		Stack<Pair<Long, Long, Long>> stack = measurementInsertMap.get(type).get(time);
		for(int j = stack.size()-1; j >= 0; j--){
			Pair<Long, Long, Long> current = stack.get(j);
			if(last == null)
				last = current;
			else if(matchKeys(current, last)){
				last = current;
			}else{
				return last.getY();
			}
		}

		return measurementInsertMap.get(type).get(time).lastElement().getY();
	}
	
//	public Collection<Long> getAllDelays(OperationType type, long time){
//		return new ArrayList<Long>(measurementInsertMap.get(type).get(time));
//	}
	
	private boolean matchKeys(Pair<Long, Long, Long> current,
			Pair<Long, Long, Long> last) {
		if(last.getZ() == null){
			return current.getZ() == null;
		}
		
		return last.getZ().equals(current.getZ());
	}

	public int getNumberOfDelays(OperationType type, long time){
		if(!measurementInsertMap.containsKey(type))
			return 0;
		if(!measurementInsertMap.get(type).containsKey(time))
			return 0;
		
		int number = 0;
		Pair<Long, Long, Long> last = null;
		Stack<Pair<Long, Long, Long>> stack = measurementInsertMap.get(type).get(time);
		for(int j = stack.size()-1; j >= 0; j--){
			Pair<Long, Long, Long> current = stack.get(j);
			if(last == null){
				number++;
			}
			else if(!matchKeys(current, last)){
				number++;
			}
			last = current;
		}

		return number;
	}

	public Stack<Pair<Long, Long, Long>>  getAllValues(OperationType type, long time) {
		if(!measurementInsertMap.containsKey(type))
			return new Stack<Pair<Long, Long, Long>>();
		if(!measurementInsertMap.get(type).containsKey(time))
			return new Stack<Pair<Long, Long, Long>>();
		
		
		return measurementInsertMap.get(type).get(time);
	}
}
