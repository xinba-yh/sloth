package com.tsingj.sloth.store;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.replication.LogStateMachine;
import com.tsingj.sloth.store.utils.CommonUtil;
import com.tsingj.sloth.store.utils.StoragePathHelper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;

/**
 * @author yanghao
 */
@Component
public class RaftReplicationServer {

    private static final Logger logger = LoggerFactory.getLogger(RaftReplicationServer.class);

    private final StorageProperties storageProperties;

    private final StoragePathHelper storagePathHelper;


    //-----jraft 相关-----


    private RaftGroupService raftGroupService;

    private final LogStateMachine logStateMachine;

    private Node node;


    public RaftReplicationServer(StorageProperties storageProperties, StoragePathHelper storagePathHelper, LogStateMachine logStateMachine) {
        this.storageProperties = storageProperties;
        this.storagePathHelper = storagePathHelper;
        this.logStateMachine = logStateMachine;
    }

    public void start() throws IOException {
        final String raftBaseDir = storagePathHelper.getRaftLogDir();
        final String groupId = storageProperties.getRaftGroup();
        final String serverId = storageProperties.getRaftServerId();
        final String initConfStr = storageProperties.getRaftServerList();

        final NodeOptions nodeOptions = new NodeOptions();
        // for test, modify some params
        // set election timeout to 1s
        nodeOptions.setElectionTimeoutMs(1000);
        // disable CLI service。
        nodeOptions.setDisableCli(false);
        // parse server address
        final PeerId peerId = new PeerId();
        if (!peerId.parse(serverId)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverId);
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // set cluster configuration
        nodeOptions.setInitialConf(initConf);

        // start raft server
        // init raft data path, it contains log,meta,snapshot
        FileUtils.forceMkdir(new File(raftBaseDir));

        // here use same RPC server for raft and business. It also can be seperated generally
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(peerId.getEndpoint());
//        // GrpcServer need init marshaller
//        CounterGrpcHelper.initGRpc();
//        CounterGrpcHelper.setRpcServer(rpcServer);

        // set fsm to nodeOptions
        nodeOptions.setFsm(this.logStateMachine);
        // set storage path (log,meta,snapshot)
        // log, must
        String logDir = raftBaseDir + File.separator + "log";
        CommonUtil.createDirIfNotExists(logDir);
        nodeOptions.setLogUri(logDir);
        // meta, must
        String raftMetaDir = raftBaseDir + File.separator + "raft_meta";
        CommonUtil.createDirIfNotExists(raftMetaDir);
        nodeOptions.setRaftMetaUri(raftMetaDir);
//         snapshot, optional, generally recommended
//        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // init raft group service framework
        this.raftGroupService = new RaftGroupService(groupId, peerId, nodeOptions, rpcServer);
        // start raft node
        this.node = this.raftGroupService.start();

        logger.info("Started replication server at port:{}.", this.getNode().getNodeId().getPeerId().getPort());
    }

    public LogStateMachine getFsm() {
        return this.logStateMachine;
    }

    public Node getNode() {
        return this.node;
    }


    @PreDestroy
    public void destroy() {
        this.raftGroupService.shutdown();
    }

}
