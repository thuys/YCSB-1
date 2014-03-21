package com.yahoo.ycsb.util;

public class Pair<K, L> {
	private final K x;
	private final L y;
	
	
	public Pair(K x, L y) {
		super();
		this.x = x;
		this.y = y;
	}
	
	public K getX() {
		return x;
	}
	public L getY() {
		return y;
	}
}
