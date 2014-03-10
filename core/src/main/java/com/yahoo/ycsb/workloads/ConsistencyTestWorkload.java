package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
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
	private static final String SEED_PROPERTY = "seed";
	private static final String DEFAULT_SEED_PROPERTY = "4634514122364";
	private static final String STRING_CONSISTENCY_DELAY = "consistencyDelay";
	
	ScheduledThreadPoolExecutor executor;
	
	private Random timepointForOperationGenerator;
	private long nextTimestamp;
	private static final String FIELD_WITH_TIMESTAMP = "field0";
	private int keyCounter;
	private ConsistencyOneMeasurement oneMeasurement;
	private long delayBetweenConsistencyChecks;
	
	public ConsistencyTestWorkload(){
		this.timepointForOperationGenerator = null;
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
		String seedAsString = p.getProperty(SEED_PROPERTY, DEFAULT_SEED_PROPERTY);
		long seed = this.convertToLong(seedAsString, "Property \"" + SEED_PROPERTY + "\" should be an integer");
		this.timepointForOperationGenerator = new Random(seed);
		this.keyCounter = 0;
		
		if(!p.containsKey(STRING_CONSISTENCY_DELAY)){
			throw new WorkloadException("Not consistency delay defined: " + STRING_CONSISTENCY_DELAY);
		}
		this.delayBetweenConsistencyChecks = this.convertToLong(p.getProperty(STRING_CONSISTENCY_DELAY), 
				"Property \"" + STRING_CONSISTENCY_DELAY + "\" should be an long number");
	}
	
	private void updateTimestamp(){
		// Move timestamp with value in [1,10] seconds
		// TODO: make interval adjustable
		this.nextTimestamp = this.nextTimestamp + ((this.timepointForOperationGenerator.nextInt(10)+1)*1000);
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
		return "user" + keynum;
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
		
		long initialDelay = this.nextTimestamp - System.nanoTime()/1000;
		executor.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				boolean consistencyReached = false;
				HashMap<String, ByteIterator> readResult = new HashMap<String,ByteIterator>();
				db.read(table, keyname,fields, readResult);
				consistencyReached = isConsistencyReached(readResult);
				if(consistencyReached){
					long time = Long.parseLong(readResult.get(FIELD_WITH_TIMESTAMP).toString());
					long delay = System.nanoTime()/1000 - time;
					System.err.println("consistency reached!!!");
					
					oneMeasurement.addMeasurement(time, delay/1000);
					
					// Remove 
					executor.remove(this);
					
					
				}
				//TODO Check for time out
			}
		}, initialDelay, delayBetweenConsistencyChecks, TimeUnit.MICROSECONDS);
		this.updateTimestamp();
	}
	
	
	private boolean isConsistencyReached(HashMap<String, ByteIterator> readResult){
		ByteIterator readValueAsByteIterator = readResult.get(FIELD_WITH_TIMESTAMP);
		if(readValueAsByteIterator == null){
			return false;
		}
		long readValue= Long.parseLong(readValueAsByteIterator.toString());
		return (this.nextTimestamp == readValue);
	}
	
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

	public void doTransactionInsert(DB db){
		int keynum= nextKeynum();
		String dbkey = buildKeyName(keynum);
		HashMap<String, ByteIterator> values = buildValues();
		this.sleepUntilTimestampReached(this.nextTimestamp);
		///////////
		System.err.println("WRITER_THREAD: inserting values: " + values +  " for key: " + dbkey);
		///////////
		db.insert(table,dbkey,values);
		this.updateTimestamp();
	}
	
	private void sleepUntilTimestampReached(long timestamp){
		long sleepTime = timestamp - System.currentTimeMillis();
		/////////
		System.err.println("sleep during: " + sleepTime + ", waiting for timestamp: " + timestamp);
		/////////
		if(sleepTime > 0){
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public ConsistencyOneMeasurement getOneMeasurement() {
		return oneMeasurement;
	}

	public void setOneMeasurement(int threadID) {
		this.oneMeasurement = new ConsistencyOneMeasurement(threadID);
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