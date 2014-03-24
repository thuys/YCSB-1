package com.yahoo.ycsb.workloads.runners;

public class LastReadValue {
	private boolean readValue;
	private boolean readKey;
	private long key;
	
	public LastReadValue(){
		readKey = false;
		readValue = false;
	}
	
	public boolean hasReadValue() {
		return readValue;
	}
	public void setReadValue(boolean readValue) {
		this.readValue = readValue;
	}
	public boolean hasReadKey() {
		return readKey;
	}

	public void setReadKey(boolean readKey) {
		setReadValue(true);
		this.readKey = readKey;
	}
	
	private long getKey() {
		return key;
	}
	public void setKey(long key) {
		setReadValue(true);
		setReadKey(true);
		this.key = key;
	}
	
	public boolean checkKey(long key){
		if(!hasReadValue())
			return false;
		if(!hasReadKey())
			return false;
		
		return key == getKey();
	}
}
