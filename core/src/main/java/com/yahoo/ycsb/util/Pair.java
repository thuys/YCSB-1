package com.yahoo.ycsb.util;

public class Pair<K, L, M> {
	private final K x;
	private final L y;
	private final M z;

	
	public Pair(K x, L y, M z) {
		super();
		this.x = x;
		this.y = y;
		this.z = z;
	}
	
	public K getX() {
		return x;
	}
	public L getY() {
		return y;
	}
	
	public M getZ() {
		return z;
	}
}
