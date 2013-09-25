package com.quickserverlab.quickcached.client;

public class CASValue {
	private final long cas;
	private final Object value;

	public CASValue(long c, Object v) {
		cas = c;
		value = v;
	}

	public long getCas() {
		return cas;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "[{CasValue " + cas + "/" + value + "}";
	}
}
