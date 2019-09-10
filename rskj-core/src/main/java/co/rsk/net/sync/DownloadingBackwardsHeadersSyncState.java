package co.rsk.net.sync;

import co.rsk.crypto.Keccak256;
import co.rsk.net.NodeID;
import co.rsk.scoring.EventType;
import org.ethereum.core.Block;
import org.ethereum.core.BlockHeader;
import org.ethereum.db.BlockStore;

import java.util.*;

public class DownloadingBackwardsHeadersSyncState extends BaseSyncState {

    private final BlockStore blockStore;
    private final NodeID selectedPeerId;

    public DownloadingBackwardsHeadersSyncState(
            SyncConfiguration syncConfiguration,
            SyncEventsHandler syncEventsHandler,
            BlockStore blockStore,
            NodeID selectedPeerId) {
        super(syncEventsHandler, syncConfiguration);
        this.blockStore = blockStore;
        this.selectedPeerId = selectedPeerId;
    }

    @Override
    public void newBlockHeaders(List<BlockHeader> toRequest) {
        syncEventsHandler.backwardDownloadBodies(selectedPeerId, toRequest);
    }

    @Override
    public void onEnter() {
        requestHeaders();
    }

    private void requestHeaders() {
        Block block = blockStore.getChainBlockByNumber(blockStore.getMinNumber());
        Keccak256 hashToRequest = block.getHash();
        ChunkDescriptor chunkDescriptor = new ChunkDescriptor(
                hashToRequest.getBytes(),
                syncConfiguration.getChunkSize());

        boolean sent = syncEventsHandler.sendBlockHeadersRequest(chunkDescriptor, selectedPeerId);
        if (!sent) {
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
