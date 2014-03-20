package com.yahoo.ycsb.workloads.runners;

import java.util.HashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.workloads.ConsistencyTestWorkload;

public abstract class WriterRunner implements Runnable{

	protected final DB db;
	protected final String dbKey;
	protected final HashMap<String, ByteIterator> values;
	
	protected final ConsistencyTestWorkload workload;
	
	public WriterRunner(DB db, String dbKey, HashMap<String, ByteIterator> values, ConsistencyTestWorkload workload){
		this.db = db;
		this.dbKey = dbKey;
		this.values = values;
		this.workload = workload;
	}
	
	public void run() {
		try {
			// /////////
			System.err.println("WRITER_THREAD: inserting values: "
					+ this.values + " for key: " + this.dbKey + " at " + (System.nanoTime()/1000));
			// /////////
			this.doRun();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	public abstract void doRun();
}