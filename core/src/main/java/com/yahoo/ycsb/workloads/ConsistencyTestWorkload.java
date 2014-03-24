package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.ConsistencyOneMeasurement;

public abstract class ConsistencyTestWorkload extends CoreWorkload {

	private static final String START_POINT_PROPERTY = "starttime";
	private static final String DEFAULT_START_POINT_PROPERTY = "10000";
	private static final String CONSISTENCY_DELAY_PROPERTY = "consistencyDelayMillis";
	public static final String NEW_REQUEST_PERIOD_PROPERTY = "newrequestperiodMillis";
	
	public static final String MAX_DELAY_BEFORE_DROP = "maxDelayConsistencyBeforeDropInMicros";
	public static final String MAX_DELAY_BEFORE_DROP_DEFAULT = "1000000000";

	public static final String TIMEOUT_BEFORE_DROP = "timeoutConsistencyBeforeDropInMicros";
	public static final String TIMEOUT_BEFORE_DROP_DEFAULT = "1000000000";
	
	public static final String STOP_ON_FIRST_CONSISTENCY = "stopOnFirstConsistency";
	public static final String STOP_ON_FIRST_CONSISTENCY_DEFAULT = "true";

	ScheduledThreadPoolExecutor executor;

	protected long nextTimestamp;
	protected static final String FIELD_WITH_TIMESTAMP = "field1";
	protected ConsistencyOneMeasurement oneMeasurement;
	protected long delayBetweenConsistencyChecks;
	protected boolean firstOperation;
	
	private int keyCounter;
	private long newRequestPeriod;
	private Random randomForUpdateOperations;
	
	private long maxDelayBeforeDropQuery;
	private long timeoutBeforeDrop;
	private boolean stopOnFirstConsistency;
	
	protected long getMaxDelayBeforeDrop() {
		return maxDelayBeforeDropQuery;
	}
	
	protected boolean isStopOnFirstConsistency() {
		return stopOnFirstConsistency;
	}

	protected long getTimeOut() {
		return timeoutBeforeDrop;
	}
	
	public ConsistencyTestWorkload() {
		this.nextTimestamp = -1;
		this.keyCounter = 0;
		executor = new ScheduledThreadPoolExecutor(1);
		// TODO: set seed
		this.randomForUpdateOperations = new Random(1533447432334L);
		this.firstOperation = true;
	}

	public void init(Properties p) throws WorkloadException {
		super.init(p);
		String startTimeAsString = p.getProperty(START_POINT_PROPERTY,
				DEFAULT_START_POINT_PROPERTY);
		String synchronousClockAsString = p.getProperty("synchronousClock");
		this.nextTimestamp = this.convertToLong(synchronousClockAsString,
				"Illegal synchronousClock value")
				+ this.convertToLong(startTimeAsString, "Property \""
						+ START_POINT_PROPERTY
						+ "\" should be an integer number")*1000;
		
		//System.err.println("FIRST NEXT TIMESTAMP: " + this.nextTimestamp);
		//System.err.println("CURRENT TIME: " + System.nanoTime()/1000);
		
		this.keyCounter = 0;

		if (!p.containsKey(CONSISTENCY_DELAY_PROPERTY)) {
			throw new WorkloadException("Not consistency delay defined: "
					+ CONSISTENCY_DELAY_PROPERTY);
		}
		this.delayBetweenConsistencyChecks = 1000 * this.convertToLong(
				p.getProperty(CONSISTENCY_DELAY_PROPERTY), "Property \""
						+ CONSISTENCY_DELAY_PROPERTY
						+ "\" should be an long number");
		this.newRequestPeriod = 1000 * this.convertToLong(
				p.getProperty(NEW_REQUEST_PERIOD_PROPERTY), "Property \""
						+ NEW_REQUEST_PERIOD_PROPERTY
						+ "\" should be an long number");
		
		this.maxDelayBeforeDropQuery = this.convertToLong(
				p.getProperty(MAX_DELAY_BEFORE_DROP, MAX_DELAY_BEFORE_DROP_DEFAULT), "Property \""
						+ NEW_REQUEST_PERIOD_PROPERTY
						+ "\" should be an long number");
		this.stopOnFirstConsistency = this.convertToBoolean(
				p.getProperty(STOP_ON_FIRST_CONSISTENCY, STOP_ON_FIRST_CONSISTENCY_DEFAULT), "Property \""
						+ STOP_ON_FIRST_CONSISTENCY
						+ "\" should be a boolean");
		this.timeoutBeforeDrop = this.convertToLong(
				p.getProperty(TIMEOUT_BEFORE_DROP, TIMEOUT_BEFORE_DROP_DEFAULT), "Property \""
						+ TIMEOUT_BEFORE_DROP
						+ "\" should be an long number");
	}

	protected void updateTimestamp() {
		this.nextTimestamp = this.nextTimestamp + this.newRequestPeriod;
	}

	HashMap<String, ByteIterator> buildValues() {
		HashMap<String, ByteIterator> values = super.buildValues();
		String fieldkey = FIELD_WITH_TIMESTAMP;
		String nextTimestampAsString = Long.toString(this.nextTimestamp);
		ByteIterator data = new StringByteIterator(nextTimestampAsString);
		values.put(fieldkey, data);
		return values;
	}

	int nextKeynum() {
		return this.keyCounter++;
	}

	protected String buildKeyForUpdate(){
		int keynum = this.randomForUpdateOperations.nextInt(this.keyCounter);
		return "consistency" + keynum;
	}
	
	public String buildKeyName(long keynum) {
		return "consistency" + keynum;
	}

	HashMap<String, ByteIterator> buildUpdate() {
		return this.buildValues();
	}

	private long convertToLong(String stringToConvert, String errorMessage)
			throws WorkloadException {
		if (stringToConvert == null)
			throw new WorkloadException(errorMessage);
		try {
			return Long.parseLong(stringToConvert);
		} catch (NumberFormatException exc) {
			throw new WorkloadException(errorMessage);
		}
	}
	
	private boolean convertToBoolean(String stringToConvert, String errorMessage)
			throws WorkloadException {
		if (stringToConvert == null)
			throw new WorkloadException(errorMessage);
		try {
			return Boolean.parseBoolean(stringToConvert);
		} catch (NumberFormatException exc) {
			throw new WorkloadException(errorMessage);
		}
	}

	public void doTransactionRead(DB db) {
		throw new UnsupportedOperationException("read not supported");
	}

	// private boolean isConsistencyReached(HashMap<String, ByteIterator>
	// readResult, long expectedValue){
	// ByteIterator readValueAsByteIterator =
	// readResult.get(FIELD_WITH_TIMESTAMP);
	// if(readValueAsByteIterator == null){
	// System.err.println("expected: null " + expectedValue);
	// return false;
	// }
	// String temp = readValueAsByteIterator.toString().trim();
	//
	//
	// long readValue= Long.parseLong(temp);
	// return (expectedValue == readValue);
	// }

	public void doTransactionReadModifyWrite(DB db) {
		throw new UnsupportedOperationException(
				"read-modify-write not supported");
	}

	public void doTransactionScan(DB db) {
		throw new UnsupportedOperationException("scan not supported");
	}

	public void doTransactionUpdate(DB db) {
		throw new UnsupportedOperationException("update not supported");
	}

	public void doTransactionInsert(final DB db) {
		throw new UnsupportedOperationException("insert not supported");
	}
	
	protected void scheduleRunnableOnNextTimestamp(Runnable runnable){
		long sleepTime = this.nextTimestamp - (System.nanoTime() / 1000);
		this.executor.schedule(runnable, sleepTime, TimeUnit.MICROSECONDS);
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

	public String getFieldWithTimestamp(){
		return FIELD_WITH_TIMESTAMP;
	}
	
	public String getTableName(){
		return table;
	}
	
	@Override
	public void cleanup() throws WorkloadException {
		super.cleanup();
		try {
			executor.awaitTermination(2000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			//System.err.println("Consistency workload not stopped");
		}
	}
	
	protected long getNextTimeStamp() {
		return this.nextTimestamp;
	}
	
	public int getKeyCounter(){
		return this.keyCounter;
	}
	
	public long getNewRequestPriodInMicros(){
		return this.newRequestPeriod;
	}
	
}