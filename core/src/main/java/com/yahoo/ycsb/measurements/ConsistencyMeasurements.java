package com.yahoo.ycsb.measurements;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistencyMeasurements {
	private Set<ConsistencyOneMeasurement> allReadMeasurements;
	private Set<ConsistencyOneMeasurement> allWriteMeasurements;

	
	private static final String INSERT_MATRIX_DELAY_PROPERTY = "insertMatrixDelayExportFile";
	private static final String UPDATE_MATRIX_DELAY_PROPERTY = "updateMatrixDelayExportFile";
	private static final String INSERT_MATRIX_NB_OF_CHANGES_PROPERTY = "insertMatrixNbOfChangesExportFile";
	private static final String UPDATE_MATRIX_NB_OF_CHANGES_PROPERTY = "updateMatrixNbOfChangesExportFile";
	
	
	private final static String SEPERATOR = ",";

	public ConsistencyMeasurements() {
		this.allReadMeasurements = new HashSet<ConsistencyOneMeasurement>();
		this.allWriteMeasurements = new HashSet<ConsistencyOneMeasurement>();
	}

	public void addMeasurement(ConsistencyOneMeasurement measurement) {
		allReadMeasurements.add(measurement);
	}

	public TreeSet<Long> getAllTimings(OperationType type){
		TreeSet<Long> mergedKeys = new TreeSet<Long>();

		for (ConsistencyOneMeasurement measurement : allReadMeasurements) {
			mergedKeys.addAll(measurement.getTimes(type));
		}
		return mergedKeys;
	}
	
	public TreeMap<Long, TreeMap<Integer, Long>> exportMeasurements(OperationType type) {
		TreeSet<Long> mergedKeys = getAllTimings(type);

		TreeMap<Long, TreeMap<Integer, Long>> result = new TreeMap<Long, TreeMap<Integer, Long>>();
		for (Long time : mergedKeys) {
			TreeMap<Integer, Long> timeMap = new TreeMap<Integer, Long>();
			for (ConsistencyOneMeasurement measurement : allReadMeasurements) {
				if (measurement.hasDelay(type, time))
					timeMap.put(measurement.getThreadNumber(),
							measurement.getLastDelay(type, time));

				result.put(time, timeMap);
			}
		}

		return result;
	}
	
	public String exportLastDelaysAsMatrix(final OperationType type){
		return exportString(type, new ExportDelay() {
			
			@Override
			public String export(Long time, ConsistencyOneMeasurement measurement) {
				return Long.toString(measurement.getLastDelay(type, time));
			}
		});
	}
	
	public String exportNbOfDifferentDelaysAsMatrix(final OperationType type){
		return exportString(type, new ExportDelay() {
			
			@Override
			public String export(Long time, ConsistencyOneMeasurement measurement) {
				return Integer.toString(measurement.getNumberOfDelays(type, time));
			}
		});
	}
	
	private String exportString(OperationType type, ExportDelay export){
		String output = "";
		
		// First line with header
		Set<Integer> threadIds = new TreeSet<Integer>();
		output += SEPERATOR;
		
		for(ConsistencyOneMeasurement measurement : allReadMeasurements){
			threadIds.add(measurement.getThreadNumber());
			output += measurement.getThreadNumber() + SEPERATOR;
		}
		output += "\n";
		
		for(Long time :getAllTimings(type)){
			output += time + SEPERATOR;
			for(ConsistencyOneMeasurement measurement : allReadMeasurements){
				if(measurement.hasDelay(type, time)){
					output += export.export(time, measurement);
				}
				output += SEPERATOR;
			}
			
			output += "\n";
		}
		
		return output;
	}
	
	public ConsistencyOneMeasurement getNewReadConsistencyOneMeasurement(){
		ConsistencyOneMeasurement result = new ConsistencyOneMeasurement(allReadMeasurements.size());
		
		allReadMeasurements.add(result);
		return result;
	}
	
	public ConsistencyOneMeasurement getNewWriteConsistencyOneMeasurement(){
		ConsistencyOneMeasurement result = new ConsistencyOneMeasurement(allWriteMeasurements.size());
		
		allWriteMeasurements.add(result);
		return result;
	}
	
	private interface ExportDelay{
		public String export(Long time, ConsistencyOneMeasurement measurement);
	}

	public void export(Properties props) {
		System.err.println("STARTING TO EXPORT");
		
		if(props.getProperty(INSERT_MATRIX_DELAY_PROPERTY) != null){
			System.err.println("\tSTARTING TO EXPORT INSERT DELAY");

			try {
				PrintWriter out = new PrintWriter(props.getProperty(INSERT_MATRIX_DELAY_PROPERTY));
				out.println(exportLastDelaysAsMatrix(OperationType.INSERT));
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			System.err.println("\tENDING EXPORT");
		}
		
		if(props.getProperty(UPDATE_MATRIX_DELAY_PROPERTY) != null){
			System.err.println("\tSTARTING TO EXPORT UPDATE DELAY");

			try {
				PrintWriter out = new PrintWriter(props.getProperty(UPDATE_MATRIX_DELAY_PROPERTY));
				out.println(exportLastDelaysAsMatrix(OperationType.UPDATE));
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			System.err.println("\tENDING EXPORT");
		}
		
		if(props.getProperty(INSERT_MATRIX_NB_OF_CHANGES_PROPERTY) != null){
			System.err.println("\tSTARTING TO EXPORT INSERT NB OF CHANGES");

			try {
				PrintWriter out = new PrintWriter(props.getProperty(INSERT_MATRIX_NB_OF_CHANGES_PROPERTY));
				out.println(exportNbOfDifferentDelaysAsMatrix(OperationType.INSERT));
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			System.err.println("\tENDING EXPORT");
		}
		
		if(props.getProperty(UPDATE_MATRIX_NB_OF_CHANGES_PROPERTY) != null){
			System.err.println("\tSTARTING TO EXPORT UPDATE NB OF CHANGES");

			try {
				PrintWriter out = new PrintWriter(props.getProperty(UPDATE_MATRIX_NB_OF_CHANGES_PROPERTY));
				out.println(exportNbOfDifferentDelaysAsMatrix(OperationType.UPDATE));
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			System.err.println("\tENDING EXPORT");
		}

		
	}
}
