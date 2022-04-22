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
package com.tsingj.sloth.store.replication;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.tsingj.sloth.common.ProtoStuffSerializer;
import com.tsingj.sloth.store.datalog.DataLog;
import com.tsingj.sloth.store.pojo.Message;
import com.tsingj.sloth.store.pojo.PutMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
@Component
public class LogStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(LogStateMachine.class);

    private final AtomicLong seq = new AtomicLong();

    private final DataLog dataLog;

    /**
     * Leader term
     */
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public LogStateMachine(DataLog dataLog) {
        this.dataLog = dataLog;
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }


    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            seq.incrementAndGet();
            Status status = Status.OK();
            LogOperation logOperation;
            LogClosure closure = null;
            try {
                //iter.done() != null 说明当前状态机是Leader，直接从closure中获取操作数据
                if (iter.done() != null) {
                    closure = (LogClosure) iter.done();
                    logOperation = closure.getLogOperation();
                }
                //当前状态机不是Leader，从raft log中的数据获取logOperation
                else {
                    final ByteBuffer data = iter.getData();
                    logOperation = ProtoStuffSerializer.deserialize(data.array(), LogOperation.class);
                }

                //process operation
                byte type = logOperation.getType();
                byte[] logData = logOperation.getData();
                byte[] response = null;
                if (type == LogOperation.APPEND_MESSAGE) {
                    Message message = ProtoStuffSerializer.deserialize(logData, Message.class);
                    PutMessageResult putMessageResult = dataLog.putMessage(message);
                    //if status not success throw it ?
                    response = ProtoStuffSerializer.serialize(putMessageResult);
                }

                if (closure != null) {
                    closure.setResponse(response);
                }

                if (seq.get() % 1000 == 0) {
                    LOG.info("onApply closure {} {}.", seq.get(), closure != null);
                }
            } catch (Throwable e) {
                LOG.error("onApply error", e);
                status.setError(RaftError.UNKNOWN, e.toString());
                throw e;
            } finally {
                Optional.ofNullable(closure).ifPresent(closure1 -> closure1.run(status));
            }
            //将raft log的消费位置+1，表示当前这个raft log已经被成功消费了。
            iter.next();
        }
    }


    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e, e);
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

}
