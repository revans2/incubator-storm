package org.apache.storm.metrics2.store;

public interface IScanCallback {
    public boolean cb(byte[] key, byte[] val);
}
