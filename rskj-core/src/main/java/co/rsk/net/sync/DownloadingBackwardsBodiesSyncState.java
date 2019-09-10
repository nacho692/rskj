package co.rsk.net.sync;

import co.rsk.core.BlockDifficulty;
import co.rsk.crypto.Keccak256;
import co.rsk.net.MessageChannel;
import co.rsk.net.NodeID;
import co.rsk.net.messages.BodyResponseMessage;
import co.rsk.scoring.EventType;
import org.ethereum.core.Block;
import org.ethereum.core.BlockFactory;
import org.ethereum.core.BlockHeader;
import org.ethereum.db.BlockStore;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DownloadingBackwardsBodiesSyncState extends BaseSyncState {

    private final BlockFactory blockFactory;
    private final BlockStore blockStore;
    private final Queue<BlockHeader> toRequest;
    private final NodeID selectedPeerId;

    private Block parent;
    private BlockHeader inTransit;

    public DownloadingBackwardsBodiesSyncState(
            SyncConfiguration syncConfiguration,
            SyncEventsHandler syncEventsHandler,
            BlockFactory blockFactory,
            BlockStore blockStore,
            List<BlockHeader> toRequest,
            NodeID selectedPeerId) {

        super(syncEventsHandler, syncConfiguration);
        this.blockFactory = blockFactory;
        this.blockStore = blockStore;
        this.toRequest = new LinkedList<>(toRequest);
        this.parent = blockStore.getChainBlockByNumber(blockStore.getMinNumber());
        this.selectedPeerId = selectedPeerId;
    }

    @Override
    public void newBody(BodyResponseMessage body, MessageChannel peer) {
        Block block = blockFactory.newBlock(inTransit, body.getTransactions(), body.getUncles());
        block.seal();

        if (!block.getHash().equals(inTransit.getHash()) && !parent.isParentOf(block)) {
            syncEventsHandler.onErrorSyncing(
                    peer.getPeerNodeID(),
                    "Incorrect body response to header: {}, parent: {} by node {}",
                    EventType.INVALID_BLOCK,
                    inTransit.getHash(),
                    parent.getHash(),
                    selectedPeerId);
        }

        BlockDifficulty blockchainDifficulty = blockStore.getTotalDifficultyForHash(parent.getHash().getBytes());
        blockStore.saveBlock(block, blockchainDifficulty.subtract(block.getCumulativeDifficulty()), true);
        parent = block;

        if (!toRequest.isEmpty()) {
            requestBody();
            return;
        }

        blockStore.flush();
        syncEventsHandler.stopSyncing();
    }

    @Override
    public void onEnter() {
        requestBody();
    }

    private void requestBody() {
        inTransit = toRequest.remove();

        Long sent = syncEventsHandler.sendBodyRequest(inTransit, selectedPeerId);
        if (sent == null) {
            syncEventsHandler.onSyncIssue("Channel failed to sent on {} to {}",
                    this.getClass(), selectedPeerId);
        }
    }

    @Override
    protected void onMessageTimeOut() {
        syncEventsHandler.onErrorSyncing(selectedPeerId,
                "Timeout waiting requests {}", EventType.TIMEOUT_MESSAGE, this.getClass(), selectedPeerId);
    }
}
