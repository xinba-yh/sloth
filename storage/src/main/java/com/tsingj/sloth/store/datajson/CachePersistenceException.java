package com.tsingj.sloth.store.datajson;

/**
 * @author yanghao
 *
 */
public class CachePersistenceException extends RuntimeException {


    public CachePersistenceException(String message, Throwable e) {
        super(message, e);
    }

    public CachePersistenceException(Throwable e) {
        super(e);
    }

}
