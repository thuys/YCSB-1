package com.yahoo.ycsb.workloads.runners;

import java.util.HashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.workloads.ConsistencyTestWorkload;

public class InsertRunner extends WriterRunner{

	public InsertRunner(DB db, String dbKey, HashMap<String, ByteIterator> values, ConsistencyTestWorkload workload) {
		super(db, dbKey, values, workload);
	}

	@Override
	public void doRun() {
		String tableName = this.workload.getTableName();
		this.db.insert(tableName, this.dbKey, this.values);
	}

}
