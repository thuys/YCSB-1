package com.yahoo.ycsb.workloads;

import java.util.HashSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.workloads.runners.ReadRunner;

public class ReaderWorkload extends ConsistencyTestWorkload{
	
	public void doTransactionUpdate(DB db) {
		if(this.firstOperation){
			doTransactionInsert(db);
			this.firstOperation = false;
		} else{
			String dbkey = buildKeyForUpdate();
			this.checkConsistency(db, dbkey);
		}
	}

	public void doTransactionInsert(final DB db) {
		if(this.firstOperation)
			this.firstOperation = false;
		int keynum = nextKeynum();
		String dbkey = buildKeyName(keynum);
		this.checkConsistency(db, dbkey);
	}
	
	private void checkConsistency(DB db, String keyname){
		HashSet<String> fields = new HashSet<String>();
		fields.add(FIELD_WITH_TIMESTAMP);
		long currentTiming = System.nanoTime();
		long initialDelay = this.nextTimestamp - (currentTiming / 1000) + this.getDelayForThread();
		long expectedValue = this.nextTimestamp;
		
		System.err.println("Planning read at " + (System.nanoTime() / 1000) + " for " + this.nextTimestamp);
		
		ReadRunner readrunner = new ReadRunner(currentTiming, expectedValue, keyname,
											fields, db, this, this.oneMeasurement);
		ScheduledFuture<?> taskToCancel = executor.scheduleWithFixedDelay(
				readrunner, initialDelay, delayBetweenConsistencyChecks,
				TimeUnit.MICROSECONDS);
		readrunner.setTask(taskToCancel);
		this.updateTimestamp();
	}
	
}