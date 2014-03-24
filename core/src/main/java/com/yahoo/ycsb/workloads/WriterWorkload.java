package com.yahoo.ycsb.workloads;

import java.util.HashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.workloads.runners.InsertRunner;
import com.yahoo.ycsb.workloads.runners.UpdateRunner;

public class WriterWorkload extends ConsistencyTestWorkload {

	public void doTransactionUpdate(DB db) {
		if(this.firstOperation){
			this.doTransactionInsert(db);
			this.firstOperation = false;
		}
		else{
			final String dbkey = buildKeyForUpdate();
			final HashMap<String, ByteIterator> values = buildValues();
			
			long blub = (this.nextTimestamp - (System.nanoTime() / 1000));
			//System.err.println("Planning update at " + (System.nanoTime() / 1000) + " for " + blub);
			
			UpdateRunner updateRunner = new UpdateRunner(db, dbkey, values, this, oneMeasurement, getNextTimeStamp(), getMaxDelayBeforeDrop());		
			this.scheduleRunnableOnNextTimestamp(updateRunner);
		}
	}

	public void doTransactionInsert(final DB db) {
		if(this.firstOperation)
			this.firstOperation = false;
		int keynum = this.nextKeynum();
		final String dbkey = buildKeyName(keynum);
		final HashMap<String, ByteIterator> values = buildValues();
		
		long blub = (this.nextTimestamp - (System.nanoTime() / 1000));
		//System.err.println("Planning insert at " + (System.nanoTime() / 1000) + " for " + blub);
		
		InsertRunner insertRunner = new InsertRunner(db, dbkey, values, this, oneMeasurement, getNextTimeStamp(), getMaxDelayBeforeDrop());		
		this.scheduleRunnableOnNextTimestamp(insertRunner);
	}




}