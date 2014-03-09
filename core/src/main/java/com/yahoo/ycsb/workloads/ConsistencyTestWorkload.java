package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.WorkloadException;

public class ConsistencyTestWorkload extends CoreWorkload {

	private static final String START_POINT_PROPERTY = "starttime";
	private static final String DEFAULT_START_POINT_PROPERTY = "5000";
	private static final String SEED_PROPERTY = "seed";
	private static final String DEFAULT_SEED_PROPERTY = "4634514122364";
	
	private Random timepointForOperationGenerator;
	private long nextTimestamp;
	private static final String FIELD_WITH_TIMESTAMP = "field0";
	private int keyCounter;
	
	public ConsistencyTestWorkload(){
		this.timepointForOperationGenerator = null;
		this.nextTimestamp = -1;
		this.keyCounter = 0;
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
	
	public void doTransactionRead(DB db)
	{
		int keynum = nextKeynum();	
		String keyname = buildKeyName(keynum);
		HashSet<String> fields = new HashSet<String>();
		fields.add(FIELD_WITH_TIMESTAMP);
		this.sleepUntilTimestampReached(this.nextTimestamp);
		this.waitForConsistencyToBeReached(db, keyname, fields);
		this.updateTimestamp();
	}
	
	private void waitForConsistencyToBeReached(DB db, String keyname, HashSet<String> fields){
		long millisEnteringFunction = System.currentTimeMillis();
		boolean consistencyReached = false;
		boolean timeout = false;
		//////
		System.err.println("READER_THREAD: reader key " + keyname);
		//////
		while(!consistencyReached && !timeout){
			// TODO: measure delay
			HashMap<String, ByteIterator> readResult = new HashMap<String,ByteIterator>();
			db.read(table, keyname,fields, readResult);
			consistencyReached = isConsistencyReached(readResult);
			/////////////////////////////
			if(consistencyReached){
				System.err.println("consistency reached!!!");
			}
			/////////////////////////////
			timeout = hasTimeoutOccured(millisEnteringFunction);
		}
	}
	
	private boolean hasTimeoutOccured(long startTime){
		return ((System.currentTimeMillis() - startTime) > 2000);
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
	
}