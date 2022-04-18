package com.tsingj.sloth.client.springsupport;

/**
 * @author yanghao
 */
public class CommonConstants {

    public static class EventGroupMode{
        /**
         * The Constant POLL_EVENT_GROUP.
         */
        public static final int POLL_EVENT_GROUP = 0;

        /**
         * The Constant EPOLL_EVENT_GROUP.
         */
        public static final int EPOLL_EVENT_GROUP = 1;

    }

    public static class RemoteCallWay{
        /**
         * The Constant POLL_EVENT_GROUP.
         */
        public static final int NETTY = 0;

        /**
         * The Constant EPOLL_EVENT_GROUP.
         */
        public static final int GRPC = 1;

    }
}
