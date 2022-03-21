package com.tsingj.sloth.remoting;

import com.tsingj.sloth.remoting.protocol.DataPackage;

/**
 * @author yanghao
 */
public interface RemoteRequestProcessor {

    DataPackage process(DataPackage request) throws Exception;

    byte getCommand();

}
