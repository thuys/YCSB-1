package com.yahoo.ycsb.workloads.runners;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ScheduledFuture;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.measurements.ConsistencyOneMeasurement;
import com.yahoo.ycsb.workloads.ConsistencyTestWorkload;

public class ReadRunner implements Runnable {
	
	private final long identifier, expectedValue;
	private final String keyname;
	final HashSet<String> fields;
	private final DB db;
	private ScheduledFuture<?> taskToCancel;
	private final ConsistencyTestWorkload workload;
	private final ConsistencyOneMeasurement oneMeasurement;
	
	public ReadRunner(long identifier, long expectedValue, String keyname,
			HashSet<String> fields, DB db, ConsistencyTestWorkload workload, ConsistencyOneMeasurement oneMeasurement) {
		super();
		this.identifier = identifier;
		this.expectedValue = expectedValue;
		this.keyname = keyname;
		this.fields = fields;
		this.db = db;
		this.workload = workload;
		this.oneMeasurement = oneMeasurement;
	}

	public void setTask(ScheduledFuture<?> taskToCancel) {
		this.taskToCancel = taskToCancel;
	}

	@Override
	public void run() {
		try {
			System.err.println("READING_THREAD: reading key : "
					+ keyname + " for value: " + expectedValue + " at " + (System.nanoTime()/1000));

			// TODO: check of meting in measurement interval ligt
			HashMap<String, ByteIterator> readResult = new HashMap<String, ByteIterator>();
			String tableName = this.workload.getTableName();
			this.db.read(tableName, this.keyname, this.fields, readResult);
			ByteIterator readValueAsByteIterator = readResult
					.get(this.workload.getFieldWithTimestamp());

			if (readValueAsByteIterator != null) {
				String temp = readValueAsByteIterator.toString().trim();

				long time = Long.parseLong(temp);
				System.err.println("\t2\t" + temp);
				// System.err.println("queue: " + executor.getTaskCount());
				if (time == this.expectedValue) {

					long delay = System.nanoTime() / 1000 - time;

					System.err.println("consistency reached!!!");

					// TODO: hacking in de client
					this.oneMeasurement.addMeasurement(time, delay);

					// Remove
					this.taskToCancel.cancel(false);
				}
			} else {
				System.err.println("\t null ");
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		// TODO Check for time out
		System.err.println("READ RUN FINISHED");
	}
}