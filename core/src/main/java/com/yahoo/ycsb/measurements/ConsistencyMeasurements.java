package com.yahoo.ycsb.measurements;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistencyMeasurements {
	private Set<ConsistencyOneMeasurement> allMeasurements;
	private final static String SEPERATOR = ",";

	public ConsistencyMeasurements() {
		this.allMeasurements = new HashSet<ConsistencyOneMeasurement>();
	}

	public void addMeasurement(ConsistencyOneMeasurement measurement) {
		allMeasurements.add(measurement);
	}

	public TreeMap<Long, TreeMap<Integer, Long>> exportMeasurements() {
		TreeSet<Long> mergedKeys = new TreeSet<Long>();

		for (ConsistencyOneMeasurement measurement : allMeasurements) {
			mergedKeys.addAll(measurement.getTimes());
		}

		TreeMap<Long, TreeMap<Integer, Long>> result = new TreeMap<Long, TreeMap<Integer, Long>>();
		for (Long time : mergedKeys) {
			TreeMap<Integer, Long> timeMap = new TreeMap<Integer, Long>();
			for (ConsistencyOneMeasurement measurement : allMeasurements) {
				if (measurement.hasDelay(time))
					timeMap.put(measurement.getThreadNumber(),
							measurement.getDelay(time));

				result.put(time, timeMap);
			}
		}

		return result;
	}
	
	public String exportAsString(){
		TreeMap<Long, TreeMap<Integer, Long>> map = exportMeasurements();
		String output = "";
		
		// First line with header
		Set<Integer> threadIds = new TreeSet<Integer>();
		output += SEPERATOR;
		
		for(ConsistencyOneMeasurement measurement : allMeasurements){
			threadIds.add(measurement.getThreadNumber());
			output += measurement.getThreadNumber() + SEPERATOR;
		}
		output += "\n";
		
		for(Long time : map.keySet()){
			output += time + SEPERATOR;
			TreeMap<Integer, Long> innerMap = map.get(time);
			for(Integer threadId : threadIds){
				if(innerMap.containsKey(threadId)){
					output += innerMap.get(threadId);
				}
				output += SEPERATOR;
			}
			
			output += "\n";
		}
		
		return output;
	}
}
