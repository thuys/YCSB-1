package com.yahoo.ycsb.workloads.runners;

import java.util.HashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.measurements.ConsistencyOneMeasurement;
import com.yahoo.ycsb.measurements.OperationType;
import com.yahoo.ycsb.workloads.ConsistencyTestWorkload;

public abstract class WriterRunner implements Runnable {

	protected final DB db;
	protected final String dbKey;
	protected final HashMap<String, ByteIterator> values;
	protected final long timeStamp;

	protected final ConsistencyTestWorkload workload;
	protected final ConsistencyOneMeasurement measurement;

	public WriterRunner(DB db, String dbKey,
			HashMap<String, ByteIterator> values,
			ConsistencyTestWorkload workload,
			ConsistencyOneMeasurement oneMeasurement, long timeStamp) {
		this.db = db;
		this.dbKey = dbKey;
		this.values = values;
		this.workload = workload;
		this.timeStamp = timeStamp;
		this.measurement = oneMeasurement;
	}

	public void run() {
		try {
			// /////////
			long start = (System.nanoTime() / 1000);
			System.err.println("WRITER_THREAD: inserting values: "
					+ this.values + " for key: " + this.dbKey + " at "
					+ start);
			// /////////
			this.doRun();
			long delay = (System.nanoTime() / 1000) - timeStamp;
			measurement.addMeasurement(timeStamp, getType(), start, delay, timeStamp);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	protected abstract void doRun();
	
	protected abstract OperationType getType();
}