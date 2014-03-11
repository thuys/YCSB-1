package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.ConsistencyOneMeasurement;

public class ConsistencyTestWorkload extends CoreWorkload {

	private static final String START_POINT_PROPERTY = "starttime";
	private static final String DEFAULT_START_POINT_PROPERTY = "5000";
	private static final String CONSISTENCY_DELAY_PROPERTY = "consistencyDelayMillis";
	public static final String NEW_REQUEST_PERIOD_PROPERTY = "newrequestperiodMillis";
	
	ScheduledThreadPoolExecutor executor;
	
	private long nextTimestamp;
	private static final String FIELD_WITH_TIMESTAMP = "field0";
	private int keyCounter;
	private ConsistencyOneMeasurement oneMeasurement;
	private long delayBetweenConsistencyChecks;
	private long newRequestPeriod;
	
	public ConsistencyTestWorkload(){
		this.nextTimestamp = -1;
		this.keyCounter = 0;
		executor = new ScheduledThreadPoolExecutor(1);
	}
	
	public void init(Properties p) throws WorkloadException{
		super.init(p);
		String startTimeAsString = p.getProperty(START_POINT_PROPERTY, DEFAULT_START_POINT_PROPERTY);
		String synchronousClockAsString = p.getProperty("synchronousClock");
		this.nextTimestamp = this.convertToLong(synchronousClockAsString, "Illegal synchronousClock value") + 
				this.convertToLong(startTimeAsString, "Property \"" + START_POINT_PROPERTY + "\" should be an integer number");
		this.keyCounter = 0;
		
		if(!p.containsKey(CONSISTENCY_DELAY_PROPERTY)){
			throw new WorkloadException("Not consistency delay defined: " + CONSISTENCY_DELAY_PROPERTY);
		}
		this.delayBetweenConsistencyChecks = 1000*this.convertToLong(p.getProperty(CONSISTENCY_DELAY_PROPERTY), 
				"Property \"" + CONSISTENCY_DELAY_PROPERTY + "\" should be an long number");
		this.newRequestPeriod = 1000*this.convertToLong(p.getProperty(NEW_REQUEST_PERIOD_PROPERTY), 
				"Property \"" + NEW_REQUEST_PERIOD_PROPERTY + "\" should be an long number");
	}
	
	private void updateTimestamp(){
		this.nextTimestamp = this.nextTimestamp + this.newRequestPeriod;
	}
	
	HashMap<String, ByteIterator> buildValues(){
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();
 		String fieldkey = FIELD_WITH_TIMESTAMP;
 		String nextTimestampAsString = Long.toString(this.nextTimestamp);
 		ByteIterator data= new StringByteIterator(nextTimestampAsString);
 		values.put(fieldkey,data);
		return values;
	}
	
	int nextKeynum() {
		return this.keyCounter++;
	}
	
	public String buildKeyName(long keynum) {
		return "consistency" + keynum;
	}
	
	HashMap<String, ByteIterator> buildUpdate() {
		throw new UnsupportedOperationException("Update not supported");
	}
	
	private long convertToLong(String stringToConvert, String errorMessage) throws WorkloadException{
		if(stringToConvert == null)
			throw new WorkloadException(errorMessage);
		try{
			return Long.parseLong(stringToConvert);
		} catch(NumberFormatException exc){
			throw new WorkloadException(errorMessage);
		}
	}
	
	public void doTransactionRead(final DB db)
	{
		int keynum = nextKeynum();	
		final String keyname = buildKeyName(keynum);
		final HashSet<String> fields = new HashSet<String>();
		fields.add(FIELD_WITH_TIMESTAMP);
		final long currentTiming = System.nanoTime();
		long initialDelay = this.nextTimestamp - currentTiming/1000;
		final long expectedValue = this.nextTimestamp;
		executor.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try {
					System.err.println("Read run started: (at " + currentTiming + ") \n\t" + expectedValue);
					// TODO: check of meting in measurement interval ligt
					HashMap<String, ByteIterator> readResult = new HashMap<String, ByteIterator>();
					db.read(table, keyname, fields, readResult);
					ByteIterator readValueAsByteIterator = readResult.get(FIELD_WITH_TIMESTAMP);
					
					if(readValueAsByteIterator != null){
						String temp = readValueAsByteIterator.toString().trim();
						
						long time= Long.parseLong(temp);
						//System.err.println("          " + temp);
						//System.err.println("queue: " + executor.getTaskCount());
						if(time == expectedValue){

							long delay = System.nanoTime() / 1000 - time;
							
							System.err.println("consistency reached!!!");
	
							// TODO: hacking in de client
							oneMeasurement.addMeasurement(time, delay);
							
							// Remove
							executor.remove(this);
						}
					}else{
						System.err.println("\t null ");
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}
				//TODO Check for time out
				System.err.println("READ RUN FINISHED");
			}
		}, initialDelay, delayBetweenConsistencyChecks, TimeUnit.MICROSECONDS);
		this.updateTimestamp();
	}
	
	
//	private boolean isConsistencyReached(HashMap<String, ByteIterator> readResult, long expectedValue){
//		ByteIterator readValueAsByteIterator = readResult.get(FIELD_WITH_TIMESTAMP);
//		if(readValueAsByteIterator == null){
//			System.err.println("expected: null " + expectedValue);
//			return false;
//		}
//		String temp = readValueAsByteIterator.toString().trim();
//
//		
//		long readValue= Long.parseLong(temp);
//		return (expectedValue == readValue);
//	}
	
	public void doTransactionReadModifyWrite(DB db)
	{
		throw new UnsupportedOperationException("read-modify-write not supported");
	}
	
	public void doTransactionScan(DB db){
		throw new UnsupportedOperationException("scan not supported");
	}

	public void doTransactionUpdate(DB db) {
		throw new UnsupportedOperationException("update not supported");
	}

	public void doTransactionInsert(final DB db){
		int keynum= nextKeynum();
		final String dbkey = buildKeyName(keynum);
		final HashMap<String, ByteIterator> values = buildValues();
		
		long sleepTime = this.nextTimestamp - (System.nanoTime()/1000);
		executor.schedule(new Runnable() {
			
			@Override
			public void run() {
				try {
					// /////////
					System.err.println("WRITER_THREAD: inserting values: "
							+ values + " for key: " + dbkey);
					// /////////
					db.insert(table, dbkey, values);
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}, sleepTime, TimeUnit.MICROSECONDS);
		
		this.updateTimestamp();
	}

	public ConsistencyOneMeasurement getOneMeasurement() {
		return oneMeasurement;
	}

	public void setOneMeasurement(ConsistencyOneMeasurement measurement) {
		this.oneMeasurement = measurement;
	}
	
    public void requestStop() {
    	super.requestStop();
    	executor.shutdownNow();
    }
	
    @Override
    public void cleanup() throws WorkloadException{
    	super.cleanup();
    	try {
			executor.awaitTermination(2000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			System.err.println("Consistency workload not stopped");
		}
    }
}