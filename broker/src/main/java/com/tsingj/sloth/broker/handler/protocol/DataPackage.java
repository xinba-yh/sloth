package com.tsingj.sloth.broker.handler.protocol;


/**
 * @author yanghao
 */
public class DataPackage {

    public DataPackage(String magicCode, long correlationId, byte version, byte command, byte[] data) {
        this.magicCode = magicCode;
        this.correlationId = correlationId;
        this.version = version;
        this.command = command;
        this.data = data;
    }

    private String magicCode;

    private long correlationId;

    private byte version = 1;

    private byte command;

    private transient byte[] data;

    public String getMagicCode() {
        return magicCode;
    }

    public void setMagicCode(String magicCode) {
        this.magicCode = magicCode;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(long correlationId) {
        this.correlationId = correlationId;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public byte getCommand() {
        return command;
    }

    public void setCommand(byte command) {
        this.command = command;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
