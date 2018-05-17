/*
 * This file is part of RskJ
 * Copyright (C) 2017 RSK Labs Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package co.rsk;

import co.rsk.blocks.FileBlockPlayer;
import co.rsk.blocks.FileBlockRecorder;
import co.rsk.config.RskSystemProperties;
import co.rsk.core.Rsk;
import co.rsk.core.RskImpl;
import co.rsk.db.PruneConfiguration;
import co.rsk.db.PruneService;
import co.rsk.mine.MinerClient;
import co.rsk.mine.MinerServer;
import co.rsk.mine.TxBuilder;
import co.rsk.mine.TxBuilderEx;
import co.rsk.net.BlockProcessResult;
import co.rsk.net.BlockProcessor;
import co.rsk.net.MessageHandler;
import co.rsk.net.Metrics;
import co.rsk.net.discovery.UDPServer;
import co.rsk.rpc.netty.Web3HttpServer;
import org.ethereum.core.*;
import org.ethereum.net.eth.EthVersion;
import org.ethereum.net.server.ChannelManager;
import org.ethereum.net.server.PeerServer;
import org.ethereum.rpc.Web3;
import org.ethereum.sync.SyncPool;
import org.ethereum.util.BuildInfo;
import org.ethereum.vm.PrecompiledContracts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.util.stream.Collectors;

@Component
public class FullNodeRunner implements NodeRunner {
    private static Logger logger = LoggerFactory.getLogger("fullnoderunner");

    private final Rsk rsk;
    private final UDPServer udpServer;
    private final MinerServer minerServer;
    private final MinerClient minerClient;
    private final RskSystemProperties rskSystemProperties;
    private final Web3HttpServer web3HttpServer;
    private final Repository repository;
    private final Blockchain blockchain;
    private final ChannelManager channelManager;
    private final SyncPool syncPool;
    private final MessageHandler messageHandler;

    private final Web3 web3Service;
    private final BlockProcessor nodeBlockProcessor;
    private final TransactionPool transactionPool;
    private final PeerServer peerServer;
    private final SyncPool.PeerClientFactory peerClientFactory;

    private final PruneService pruneService;

    @Autowired
    public FullNodeRunner(
            Rsk rsk,
            UDPServer udpServer,
            MinerServer minerServer,
            MinerClient minerClient,
            RskSystemProperties rskSystemProperties,
            Web3 web3Service,
            Web3HttpServer web3HttpServer,
            Repository repository,
            Blockchain blockchain,
            ChannelManager channelManager,
            SyncPool syncPool,
            MessageHandler messageHandler,
            BlockProcessor nodeBlockProcessor,
            TransactionPool transactionPool,
            PeerServer peerServer,
            SyncPool.PeerClientFactory peerClientFactory) {
        this.rsk = rsk;
        this.udpServer = udpServer;
        this.minerServer = minerServer;
        this.minerClient = minerClient;
        this.rskSystemProperties = rskSystemProperties;
        this.web3HttpServer = web3HttpServer;
        this.web3Service = web3Service;
        this.repository = repository;
        this.blockchain = blockchain;
        this.channelManager = channelManager;
        this.syncPool = syncPool;
        this.messageHandler = messageHandler;
        this.nodeBlockProcessor = nodeBlockProcessor;
        this.transactionPool = transactionPool;
        this.peerServer = peerServer;
        this.peerClientFactory = peerClientFactory;

        PruneConfiguration pruneConfiguration = rskSystemProperties.getPruneConfiguration();
        this.pruneService = new PruneService(pruneConfiguration, rskSystemProperties, blockchain, PrecompiledContracts.REMASC_ADDR);
    }

    @Override
    public void run() throws Exception {
        logger.info("Starting RSK");

        logger.info("Running {},  core version: {}-{}", rskSystemProperties.genesisInfo(), rskSystemProperties.projectVersion(), rskSystemProperties.projectVersionModifier());
        BuildInfo.printInfo();

        // this should be the genesis block at this point
        transactionPool.start(blockchain.getBestBlock());
        channelManager.start();
        messageHandler.start();
        peerServer.start();

        if (logger.isInfoEnabled()) {
            String versions = EthVersion.supported().stream().map(EthVersion::name).collect(Collectors.joining(", "));
            logger.info("Capability eth version: [{}]", versions);
        }
        if (rskSystemProperties.isBlocksEnabled()) {
            setupRecorder(rskSystemProperties.blocksRecorder());
            setupPlayer(rsk, channelManager, blockchain, rskSystemProperties.blocksPlayer());
        }

        if (!"".equals(rskSystemProperties.blocksLoader())) {
            rskSystemProperties.setSyncEnabled(Boolean.FALSE);
            rskSystemProperties.setDiscoveryEnabled(Boolean.FALSE);
        }

        Metrics.registerNodeID(rskSystemProperties.nodeId());

        if (rskSystemProperties.simulateTxs()) {
            enableSimulateTxs();
        }

        if (rskSystemProperties.simulateTxsEx()) {
            enableSimulateTxsEx();
        }

        if (rskSystemProperties.isRpcEnabled()) {
            logger.info("RPC enabled");
            startRPCServer();
        } else {
            logger.info("RPC disabled");
        }

        if (rskSystemProperties.isPeerDiscoveryEnabled()) {
            udpServer.start();
        }

        if (rskSystemProperties.isSyncEnabled()) {
            syncPool.updateLowerUsefulDifficulty();
            syncPool.start(peerClientFactory);
            if (rskSystemProperties.waitForSync()) {
                waitRskSyncDone();
            }
        }

        if (rskSystemProperties.isMinerServerEnabled()) {
            minerServer.start();

            if (rskSystemProperties.isMinerClientEnabled()) {
                minerClient.mine();
            }
        }

        if (rskSystemProperties.isPruneEnabled()) {
            pruneService.start();
        }

        logger.info("done");
    }

    private void startRPCServer() throws InterruptedException {
        web3Service.start();
        web3HttpServer.start();
    }

    private void enableSimulateTxs() {
        new TxBuilder(rskSystemProperties, rsk, nodeBlockProcessor, repository).simulateTxs();
    }

    private void enableSimulateTxsEx() {
        new TxBuilderEx(rskSystemProperties, rsk, repository, nodeBlockProcessor, transactionPool).simulateTxs();
    }

    private void waitRskSyncDone() throws InterruptedException {
        while (rsk.isBlockchainEmpty() || rsk.hasBetterBlockToSync() || rsk.isPlayingBlocks()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e1) {
                logger.trace("Wait sync done was interrupted", e1);
                throw e1;
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Shutting down RSK node");

        if (rskSystemProperties.isPruneEnabled()) {
            pruneService.stop();
        }

        syncPool.stop();

        if (rskSystemProperties.isRpcEnabled()) {
            web3Service.stop();
            web3HttpServer.stop();
        }

        if (rskSystemProperties.isMinerServerEnabled()) {
            minerServer.stop();
            if (rskSystemProperties.isMinerClientEnabled()) {
                minerClient.stop();
            }
        }

        peerServer.stop();
        messageHandler.stop();
        channelManager.stop();

        if (rskSystemProperties.isPeerDiscoveryEnabled()) {
            try {
                udpServer.stop();
            } catch (InterruptedException e) {
                logger.error("Couldn't stop the updServer", e);
                Thread.currentThread().interrupt();
            }
        }

        logger.info("RSK node Shut down");
    }

    private void setupRecorder(@Nullable String blocksRecorderFileName) {
        if (blocksRecorderFileName != null) {
            blockchain.setBlockRecorder(new FileBlockRecorder(blocksRecorderFileName));
        }
    }

    private void setupPlayer(Rsk rsk, ChannelManager cm, Blockchain bc, @Nullable String blocksPlayerFileName) {
        if (blocksPlayerFileName == null) {
            return;
        }

        new Thread(() -> {
            RskImpl rskImpl = (RskImpl) rsk;
            try (FileBlockPlayer bplayer = new FileBlockPlayer(rskSystemProperties, blocksPlayerFileName)) {
                rskImpl.setIsPlayingBlocks(true);
                connectBlocks(bplayer, bc, cm);
            } catch (Exception e) {
                logger.error("Error", e);
            } finally {
                rskImpl.setIsPlayingBlocks(false);
            }
        }).start();
    }

    private void connectBlocks(FileBlockPlayer bplayer, Blockchain bc, ChannelManager cm) {
        for (Block block = bplayer.readBlock(); block != null; block = bplayer.readBlock()) {
            ImportResult tryToConnectResult = bc.tryToConnect(block);
            if (BlockProcessResult.importOk(tryToConnectResult)) {
                cm.broadcastBlock(block, null);
            }
        }
    }
}