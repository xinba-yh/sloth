/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsingj.sloth.remoting;

import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.remoting.protocol.DataPackage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author yanghao
 */
public class ResponseFuture {

    private final long correlationId;

    private final long timeoutMillis;

    private volatile DataPackage dataPackage = null;

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private volatile Throwable cause;

    private final long startTimestamp;

    public ResponseFuture(long correlationId, long timeoutMillis) {
        this.correlationId = correlationId;
        this.timeoutMillis = timeoutMillis;
        this.startTimestamp = SystemClock.now();
    }

    public boolean isTimeout() {
        return SystemClock.now() - this.startTimestamp > this.timeoutMillis;
    }

    public DataPackage waitResponse() throws InterruptedException {
        boolean await = this.countDownLatch.await(this.timeoutMillis, TimeUnit.MILLISECONDS);
        if (!await) {
            throw new InterruptedException("timeout!");
        }
        return this.dataPackage;
    }

    public void putResponse(final DataPackage dataPackage) {
        this.dataPackage = dataPackage;
        this.countDownLatch.countDown();
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

}
