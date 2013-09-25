package com.quickserverlab.quickcached.client;

/**
 *
 * @author Preetham
 */
public class GenericResponse<T> {
	private final long cas;
	private final T value;
	private final String key;

	public GenericResponse(final String key, final long cas, final T value) {
		this.cas = cas;
		this.value = value;
		this.key = key;
	}

	public long getCas() {
		return cas;
	}

	public T getValue() {
		return value;
	}
	
	public String getKey() {
		return key;
	}

	@Override
	public String toString() {
		return "[{Generic  " + cas + "/" + value + "}";
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		GenericResponse other = (GenericResponse) obj;
		if (this.cas != other.cas) {
			return false;
		}
		if (this.value == null) {
			if (other.value != null) {
				return false;
			}
		} else if (!this.value.equals(other.value)) {
			return false;
		}
		return true;
	}
	
}
