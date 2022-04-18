package com.tsingj.sloth.remoting.protocol;


import lombok.Builder;
import lombok.Data;

/**
 * @author yanghao
 */
@Builder
@Data
public class RemoteCommand {

    //-------------head-------------

    private String magicCode;

    private byte version = 1;

    private byte command;

    //-------------meta-------------

    private byte requestType;

    private Long correlationId;

    private long timestamp;

    //-------------data-------------

    private transient byte[] data;


}
