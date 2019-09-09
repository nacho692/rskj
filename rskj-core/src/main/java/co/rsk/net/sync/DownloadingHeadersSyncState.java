package co.rsk.net.sync;

import co.rsk.core.bc.ConsensusValidationMainchainView;
import co.rsk.crypto.Keccak256;
import co.rsk.net.NodeID;
import co.rsk.scoring.EventType;
import co.rsk.validators.BlockHeaderValidationRule;
import com.google.common.annotations.VisibleForTesting;
import org.ethereum.core.BlockHeader;
import org.ethereum.core.BlockIdentifier;
import org.ethereum.rpc.TypeConverter;
import org.ethereum.util.ByteUtil;
import org.ethereum.validator.DependentBlockHeaderRule;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DownloadingHeadersSyncState extends BaseSyncState {

    private final Map<NodeID, List<BlockIdentifier>> skeletons;
    private final List<Deque<BlockHeader>> pendingHeaders;
    private final ChunksDownloadHelper chunksDownloadHelper;
    private final DependentBlockHeaderRule blockParentValidationRule;
    private final BlockHeaderValidationRule blockHeaderValidationRule;
    private final boolean forwardSync;
    private final NodeID selectedPeerId;
    private Map<Keccak256, BlockHeader> pendingHeadersByHash;

    public DownloadingHeadersSyncState(
            SyncConfiguration syncConfiguration,
            SyncEventsHandler syncEventsHandler,
            ConsensusValidationMainchainView mainchainView,
            DependentBlockHeaderRule blockParentValidationRule,
            BlockHeaderValidationRule blockHeaderValidationRule,
            boolean forwardSync,
            NodeID selectedPeerId,
            Map<NodeID, List<BlockIdentifier>> skeletons,
            long connectionPoint) {
        super(syncEventsHandler, syncConfiguration);
        this.blockParentValidationRule = blockParentValidationRule;
        this.blockHeaderValidationRule = blockHeaderValidationRule;
        this.forwardSync = forwardSync;
        this.selectedPeerId = selectedPeerId;
        this.pendingHeaders = new ArrayList<>();
        this.skeletons = skeletons;
        this.chunksDownloadHelper = new ChunksDownloadHelper(syncConfiguration, skeletons.get(selectedPeerId), connectionPoint);
        this.pendingHeadersByHash = new ConcurrentHashMap<>();
        mainchainView.setPendingHeaders(pendingHeadersByHash);
    }

    @Override
    public void newBlockHeaders(List<BlockHeader> chunk) {
        Optional<ChunkDescriptor> currentChunkOpt = chunksDownloadHelper.getCurrentChunk();
        if (!currentChunkOpt.isPresent()) {
            syncEventsHandler.onSyncIssue("Current chunk not present. Node {}", selectedPeerId);
            return;
        }
        ChunkDescriptor currentChunk = currentChunkOpt.get();
        if (chunk.size() != currentChunk.getCount()
                || !ByteUtil.fastEquals(chunk.get(0).getHash().getBytes(), currentChunk.getHash())) {
            syncEventsHandler.onErrorSyncing(
                    selectedPeerId,
                    "Invalid chunk received from node {} {}", EventType.INVALID_MESSAGE,
                    selectedPeerId,
                    TypeConverter.toUnformattedJsonHex(currentChunk.getHash()));
            return;
        }

        Deque<BlockHeader> headers = new ArrayDeque<>();
        // the headers come ordered by block number desc
        // we start adding the first parent header
        BlockHeader headerToAdd = chunk.get(chunk.size() - 1);
        headers.add(headerToAdd);
        pendingHeadersByHash.put(headerToAdd.getHash(), headerToAdd);

        for (int k = 1; k < chunk.size(); ++k) {
            BlockHeader parentHeader = chunk.get(chunk.size() - k);
            BlockHeader header = chunk.get(chunk.size() - k - 1);

            if (!blockHeaderIsValid(header, parentHeader)) {
                syncEventsHandler.onErrorSyncing(selectedPeerId,
                        "Invalid header received from node {} {} {}", EventType.INVALID_HEADER,
                        header.getNumber(), header.getShortHash());
                return;
            }

            headers.add(header);
            pendingHeadersByHash.put(header.getHash(), header);
        }

        pendingHeaders.add(headers);

        if (!chunksDownloadHelper.hasNextChunk()) {
            // Finished verifying headers
            syncEventsHandler.startDownloadingBodies(pendingHeaders, skeletons, selectedPeerId, forwardSync);
            return;
        }

        resetTimeElapsed();
        trySendRequest();
    }

    @Override
    public void onEnter() {
        trySendRequest();
    }

    @VisibleForTesting
    public List<BlockIdentifier> getSkeleton() {
        return chunksDownloadHelper.getSkeleton();
    }

    private void trySendRequest() {
        boolean sent = syncEventsHandler.sendBlockHeadersRequest(chunksDownloadHelper.getNextChunk(), selectedPeerId);
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

    private boolean blockHeaderIsValid(BlockHeader header, BlockHeader parentHeader) {
        if (!parentHeader.getHash().equals(header.getParentHash())) {
            return false;
        }

        if (header.getNumber() != parentHeader.getNumber() + 1) {
            return false;
        }

        if (!blockHeaderValidationRule.isValid(header)) {
            return false;
        }

        return blockParentValidationRule.validate(header, parentHeader);
    }
}
