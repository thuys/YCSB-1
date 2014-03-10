package com.yahoo.ycsb.measurements;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistencyMeasurements {
	private Set<ConsistencyOneMeasurement> allMeasurements;
	
	private static final String INSERT_MATRIX_PROPERTY = "insertMatrixExportFile";
	
	private final static String SEPERATOR = ",";

	public ConsistencyMeasurements() {
		this.allMeasurements = new HashSet<ConsistencyOneMeasurement>();
	}

	public void addMeasurement(ConsistencyOneMeasurement measurement) {
		allMeasurements.add(measurement);
	}

	public TreeSet<Long> getAllTimings(){
		TreeSet<Long> mergedKeys = new TreeSet<Long>();

		for (ConsistencyOneMeasurement measurement : allMeasurements) {
			mergedKeys.addAll(measurement.getTimes());
		}
		return mergedKeys;
	}
	
	public TreeMap<Long, TreeMap<Integer, Long>> exportMeasurements() {
		TreeSet<Long> mergedKeys = getAllTimings();

		TreeMap<Long, TreeMap<Integer, Long>> result = new TreeMap<Long, TreeMap<Integer, Long>>();
		for (Long time : mergedKeys) {
			TreeMap<Integer, Long> timeMap = new TreeMap<Integer, Long>();
			for (ConsistencyOneMeasurement measurement : allMeasurements) {
				if (measurement.hasDelay(time))
					timeMap.put(measurement.getThreadNumber(),
							measurement.getLastDelay(time));

				result.put(time, timeMap);
			}
		}

		return result;
	}
	
	public String exportLastDelaysAsMatrix(){
		return exportString(new ExportDelay() {
			
			@Override
			public String export(Long time, ConsistencyOneMeasurement measurement) {
				return Long.toString(measurement.getLastDelay(time));
			}
		});
	}
	
	public String exportNbOfDifferentDelaysAsMatrix(){
		return exportString(new ExportDelay() {
			
			@Override
			public String export(Long time, ConsistencyOneMeasurement measurement) {
				return Integer.toString(measurement.getNumberOfDelays(time));
			}
		});
	}
	
	private String exportString(ExportDelay export){
		String output = "";
		
		// First line with header
		Set<Integer> threadIds = new TreeSet<Integer>();
		output += SEPERATOR;
		
		for(ConsistencyOneMeasurement measurement : allMeasurements){
			threadIds.add(measurement.getThreadNumber());
			output += measurement.getThreadNumber() + SEPERATOR;
		}
		output += "\n";
		
		for(Long time :getAllTimings()){
			output += time + SEPERATOR;
			for(ConsistencyOneMeasurement measurement : allMeasurements){
				if(measurement.hasDelay(time)){
					output += export.export(time, measurement);
				}
				output += SEPERATOR;
			}
			
			output += "\n";
		}
		
		return output;
	}
	
	public ConsistencyOneMeasurement getNewConsistencyOneMeasurement(){
		ConsistencyOneMeasurement result = new ConsistencyOneMeasurement(allMeasurements.size());
		
		allMeasurements.add(result);
		return result;
	}
	
	private interface ExportDelay{
		public String export(Long time, ConsistencyOneMeasurement measurement);
	}

	public void export(Properties props) {
		if(props.contains(INSERT_MATRIX_PROPERTY)){
			try {
				PrintWriter out = new PrintWriter(props.getProperty(INSERT_MATRIX_PROPERTY));
				out.println(exportLastDelaysAsMatrix());
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
	}
}
