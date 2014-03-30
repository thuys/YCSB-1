package com.yahoo.ycsb.workloads.runners;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ScheduledFuture;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.measurements.ConsistencyOneMeasurement;
import com.yahoo.ycsb.measurements.OperationType;
import com.yahoo.ycsb.workloads.ConsistencyTestWorkload;

public class ReadRunner implements Runnable {

	private final long identifier, expectedValue;
	private final String keyname;
	final HashSet<String> fields;
	private final DB db;
	private ScheduledFuture<?> taskToCancel;
	private final ConsistencyTestWorkload workload;
	private final ConsistencyOneMeasurement oneMeasurement;
	private final OperationType type;
	private final LastReadValue readValue;

	private final long maxDelayBeforeDropQuery;
	private final long period;
	private final long timeout;
	private long nextReadTime;
	
	private final boolean stopOnFirstConsistency;

	public ReadRunner(OperationType type, long identifier, long expectedValue,
			String keyname, HashSet<String> fields, DB db,
			ConsistencyTestWorkload workload,
			ConsistencyOneMeasurement oneMeasurement,
			long maxDelayBeforeDropQuery,
			long period,
			long timeout, 
			boolean stopOnFirstConsistency,
			long startTime) {
		super();
		this.identifier = identifier;
		this.expectedValue = expectedValue;
		this.keyname = keyname;
		this.fields = fields;
		this.db = db;
		this.workload = workload;
		this.oneMeasurement = oneMeasurement;
		this.type = type;
		this.readValue = new LastReadValue();
		this.maxDelayBeforeDropQuery = maxDelayBeforeDropQuery;
		this.nextReadTime = startTime;
		this.period = period;
		this.timeout = timeout;
		this.stopOnFirstConsistency = stopOnFirstConsistency;
	}

	public void setTask(ScheduledFuture<?> taskToCancel) {
		this.taskToCancel = taskToCancel;
	}

	@Override
	public void run() {
		try {
			long start = System.nanoTime() / 1000;
			long relativeStart = start- expectedValue;
			//System.err.println("READING_THREAD: reading key : " + keyname
			//		+ " for value: " + expectedValue + " at " + start);
			
			if(start > nextReadTime + maxDelayBeforeDropQuery){
				System.err.println("\tDrop of query due of time");
				oneMeasurement.addMeasurement(this.expectedValue,
						this.type, relativeStart, null, null);
				this.taskToCancel.cancel(false);
				return;
			}
			
			if(start > expectedValue + timeout){
				//System.err.println("\tTimeout of query ");
				if(!readValue.checkKey(expectedValue)){
					oneMeasurement.addMeasurement(this.expectedValue,
							this.type, relativeStart, timeout, null);
				}
				this.taskToCancel.cancel(false);
				return;
			}
			nextReadTime += period;
			
			// TODO: check of meting in measurement interval ligt
			ByteIterator readValueAsByteIterator = getReadResult();

			long delay = System.nanoTime() / 1000 - this.expectedValue;

			if (readValueAsByteIterator != null) {
				String temp = readValueAsByteIterator.toString().trim();
//				System.err.println("\t2\t" + temp);
				long time = Long.parseLong(temp);

				//if (!readValue.checkKey(time)) {
					this.oneMeasurement.addMeasurement(this.expectedValue,
							this.type, relativeStart, Math.min(delay, nextReadTime+maxDelayBeforeDropQuery), time);
				//}
				readValue.setKey(time);

				if (time == this.expectedValue) {
//					System.err.println("consistency reached!!!");
					
					if(stopOnFirstConsistency){
						// Remove
						this.taskToCancel.cancel(false);
					}
				}
			} else {
				//if (!readValue.hasReadValue() || readValue.hasReadKey()){
					this.oneMeasurement.addMeasurement(this.expectedValue,
							this.type, relativeStart, Math.min(delay, nextReadTime+maxDelayBeforeDropQuery), null);
				//}

				readValue.setReadKey(false);
//				System.err.println("\t null ");
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

	private ByteIterator getReadResult() {
		HashMap<String, ByteIterator> readResult = new HashMap<String, ByteIterator>();
		String tableName = this.workload.getTableName();
		this.db.read(tableName, this.keyname, this.fields, readResult);
		
		ByteIterator readValueAsByteIterator = readResult.get(this.workload
				.getFieldWithTimestamp());
		return readValueAsByteIterator;
	}
}