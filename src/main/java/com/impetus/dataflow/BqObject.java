package com.impetus.dataflow;

import java.io.Serializable;

public class BqObject implements Serializable {

	private static final long serialVersionUID = 1L;

	private boolean runOnce = false;

	private static BqObject bq = null;

	public static BqObject getInstance() {
		if (bq == null) {
			return bq = new BqObject();
		} else {
			return bq;
		}
	}

	public boolean isRunOnce() {
		return runOnce;
	}

	public void setRunOnce(boolean runOnce) {
		this.runOnce = runOnce;
	}

}
