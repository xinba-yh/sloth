package com.tsingj.sloth.client;


import com.tsingj.sloth.client.springsupport.RemoteProperties;

/**
 * @author yanghao
 * producer or consumer 公用一个RemoteClient
 */
public class SlothRemoteClientSingleton {

    private volatile static SlothRemoteClient INSTANCE = null;

    public static SlothRemoteClient getInstance(RemoteProperties remoteProperties){
        if(null == INSTANCE){
            synchronized(SlothRemoteClientSingleton.class){
                if(null == INSTANCE){
                    INSTANCE = new SlothRemoteClient(remoteProperties);
                }
            }
        }
        return INSTANCE;
    }

    private SlothRemoteClientSingleton(){}

}
