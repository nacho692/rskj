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

package co.rsk.core.bc;

import co.rsk.config.RskSystemProperties;
import co.rsk.core.Coin;
import co.rsk.core.TransactionExecutorFactory;
import co.rsk.crypto.Keccak256;
import co.rsk.db.RepositoryLocator;
import co.rsk.db.RepositorySnapshot;
import co.rsk.net.TransactionValidationResult;
import co.rsk.net.handler.TxPendingValidator;
import com.google.common.annotations.VisibleForTesting;
import org.ethereum.core.*;
import org.ethereum.crypto.HashUtil;
import org.ethereum.db.BlockStore;
import org.ethereum.listener.EthereumListener;
import org.ethereum.util.ByteUtil;
import org.ethereum.util.RLP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.ethereum.crypto.HashUtil.EMPTY_TRIE_HASH;
import static org.ethereum.util.BIUtil.toBI;

/**
 * Created by ajlopez on 08/08/2016.
 */
public class TransactionPoolImpl implements TransactionPool {
    private static final Logger logger = LoggerFactory.getLogger("txpool");
    private static final byte[] emptyUncleHashList = HashUtil.keccak256(RLP.encodeList(new byte[0]));

    private final TransactionSet pendingTransactions = new TransactionSet();
    private final TransactionSet queuedTransactions = new TransactionSet();

    private final Map<Keccak256, Long> transactionBlocks = new HashMap<>();
    private final Map<Keccak256, Long> transactionTimes = new HashMap<>();

    private final RskSystemProperties config;
    private final BlockStore blockStore;
    private final RepositoryLocator repositoryLocator;
    private final BlockFactory blockFactory;
    private final EthereumListener listener;
    private final TransactionExecutorFactory transactionExecutorFactory;
    private final int outdatedThreshold;
    private final int outdatedTimeout;

    private ScheduledExecutorService cleanerTimer;
    private ScheduledFuture<?> cleanerFuture;

    private Block bestBlock;

    private final TxPendingValidator validator;

    public TransactionPoolImpl(
            RskSystemProperties config,
            RepositoryLocator repositoryLocator,
            BlockStore blockStore,
            BlockFactory blockFactory,
            EthereumListener listener,
            TransactionExecutorFactory transactionExecutorFactory,
            int outdatedThreshold,
            int outdatedTimeout) {
        this.config = config;
        this.blockStore = blockStore;
        this.repositoryLocator = repositoryLocator;
        this.blockFactory = blockFactory;
        this.listener = listener;
        this.transactionExecutorFactory = transactionExecutorFactory;
        this.outdatedThreshold = outdatedThreshold;
        this.outdatedTimeout = outdatedTimeout;

        this.validator = new TxPendingValidator(config.getNetworkConstants(), config.getActivationConfig());

        if (this.outdatedTimeout > 0) {
            this.cleanerTimer = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "TransactionPoolCleanerTimer"));
        }
    }

    @Override
    public void start(Block initialBestBlock) {
        processBest(initialBestBlock);

        if (this.outdatedTimeout <= 0 || this.cleanerTimer == null) {
            return;
        }

        this.cleanerFuture = this.cleanerTimer.scheduleAtFixedRate(this::cleanUp, this.outdatedTimeout, this.outdatedTimeout, TimeUnit.SECONDS);
    }

    public boolean hasCleanerFuture() {
        return this.cleanerFuture != null;
    }

    public void cleanUp() {
        final long timestampSeconds = this.getCurrentTimeInSeconds();
        this.removeObsoleteTransactions(timestampSeconds - this.outdatedTimeout);
    }

    public int getOutdatedThreshold() { return outdatedThreshold; }

    public int getOutdatedTimeout() { return outdatedTimeout; }

    public Block getBestBlock() {
        return bestBlock;
    }

    @Override
    public PendingState getPendingState() {
        return getPendingState(getCurrentRepository());
    }

    private PendingState getPendingState(RepositorySnapshot currentRepository) {
        removeObsoleteTransactions(this.getCurrentBestBlockNumber(), this.outdatedThreshold, this.outdatedTimeout);
        return new PendingState(
                currentRepository,
                new TransactionSet(pendingTransactions),
                (repository, tx) -> transactionExecutorFactory.newInstance(
                        tx,
                        0,
                        bestBlock.getCoinbase(),
                        repository,
                        createFakePendingBlock(bestBlock),
                        0
                )
        );
    }

    private RepositorySnapshot getCurrentRepository() {
        return repositoryLocator.snapshotAt(getBestBlock().getHeader());
    }

    @Override
    public synchronized List<Transaction> addTransactions(final List<Transaction> txs) {
        List<Transaction> added = new ArrayList<>();

        for (Transaction tx : txs) {
            if (this.addTransaction(tx).transactionWasAdded()) {
                added.add(tx);

                Optional<Transaction> succesor = this.getQueuedSuccesor(tx);

                while (succesor.isPresent()) {
                    Transaction found = succesor.get();
                    queuedTransactions.removeTransactionByHash(found.getHash());

                    if (!this.addTransaction(found).transactionWasAdded()) {
                        break;
                    }

                    added.add(found);

                    succesor = this.getQueuedSuccesor(found);
                }
            }
        }

        if (listener != null && !added.isEmpty()) {
            EventDispatchThread.invokeLater(() -> {
                listener.onPendingTransactionsReceived(added);
                listener.onTransactionPoolChanged(TransactionPoolImpl.this);
            });
        }

        return added;
    }

    private Optional<Transaction> getQueuedSuccesor(Transaction tx) {
        BigInteger next = tx.getNonceAsInteger().add(BigInteger.ONE);

        List<Transaction> txsaccount = this.queuedTransactions.getTransactionsWithSender(tx.getSender());

        if (txsaccount == null) {
            return Optional.empty();
        }

        return txsaccount
                .stream()
                .filter(t -> t.getNonceAsInteger().equals(next))
                .findFirst();
    }

    @Override
    public synchronized TransactionPoolAddResult addTransaction(final Transaction tx) {
        RepositorySnapshot currentRepository = getCurrentRepository();
        TransactionValidationResult validationResult = shouldAcceptTx(tx, currentRepository);
        if (!validationResult.transactionIsValid()) {
            return TransactionPoolAddResult.withError(validationResult.getErrorMessage());
        }

        Keccak256 hash = tx.getHash();
        logger.trace("add transaction {} {}", toBI(tx.getNonce()), tx.getHash());

        Long bnumber = Long.valueOf(getCurrentBestBlockNumber());

        if (pendingTransactions.hasTransaction(tx)) {
            return TransactionPoolAddResult.withError("pending transaction with same hash already exists");
        }

        if (queuedTransactions.hasTransaction(tx)) {
            return TransactionPoolAddResult.withError("queued transaction with same hash already exists");
        }

        if (!isBumpingGasPriceForSameNonceTx(tx)) {
            return TransactionPoolAddResult.withError("gas price not enough to bump transaction");
        }

        transactionBlocks.put(hash, bnumber);
        final long timestampSeconds = this.getCurrentTimeInSeconds();
        transactionTimes.put(hash, timestampSeconds);

        BigInteger currentNonce = getPendingState(currentRepository).getNonce(tx.getSender());
        BigInteger txNonce = tx.getNonceAsInteger();
        if (txNonce.compareTo(currentNonce) > 0) {
            this.addQueuedTransaction(tx);

            return TransactionPoolAddResult.ok();
        }

        if (!senderCanPayPendingTransactionsAndNewTx(tx, currentRepository)) {
            // discard this tx to prevent spam
            return TransactionPoolAddResult.withError("insufficient funds to pay for pending and new transaction");
        }

        pendingTransactions.addTransaction(tx);

        if (listener != null) {
            EventDispatchThread.invokeLater(() -> {
                listener.onPendingTransactionsReceived(Collections.singletonList(tx));
                listener.onTransactionPoolChanged(TransactionPoolImpl.this);
            });
        }

        return TransactionPoolAddResult.ok();
    }

    private boolean isBumpingGasPriceForSameNonceTx(Transaction tx) {
        Optional<Transaction> oldTxWithNonce = pendingTransactions.getTransactionsWithSender(tx.getSender()).stream()
                .filter(t -> t.getNonceAsInteger().equals(tx.getNonceAsInteger()))
                .findFirst();

        if (oldTxWithNonce.isPresent()){
            //oldGasPrice * (100 + priceBump) / 100
            Coin oldGasPrice = oldTxWithNonce.get().getGasPrice();
            Coin gasPriceBumped = oldGasPrice
                    .multiply(BigInteger.valueOf(config.getGasPriceBump() + 100L))
                    .divide(BigInteger.valueOf(100));

            if (oldGasPrice.compareTo(tx.getGasPrice()) >= 0 || gasPriceBumped.compareTo(tx.getGasPrice()) > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void processBest(Block block) {
        logger.trace("Processing best block {} {}", block.getNumber(), block.getShortHash());

        if (bestBlock != null) {
            BlockFork fork = new BlockFork();
            fork.calculate(bestBlock, block, blockStore);

            for (Block blk : fork.getOldBlocks()) {
                retractBlock(blk);
            }

            for (Block blk : fork.getNewBlocks()) {
                acceptBlock(blk);
            }
        }

        removeObsoleteTransactions(block.getNumber(), this.outdatedThreshold, this.outdatedTimeout);

        bestBlock = block;

        if (listener != null) {
            EventDispatchThread.invokeLater(() -> listener.onTransactionPoolChanged(TransactionPoolImpl.this));
        }
    }

    @VisibleForTesting
    public void acceptBlock(Block block) {
        List<Transaction> txs = block.getTransactionsList();

        removeTransactions(txs);
    }

    @VisibleForTesting
    public void retractBlock(Block block) {
        List<Transaction> txs = block.getTransactionsList();

        this.addTransactions(txs);
    }

    @VisibleForTesting
    public void removeObsoleteTransactions(long currentBlock, int depth, int timeout) {
        List<Keccak256> toremove = new ArrayList<>();
        final long timestampSeconds = this.getCurrentTimeInSeconds();

        for (Map.Entry<Keccak256, Long> entry : transactionBlocks.entrySet()) {
            long block = entry.getValue().longValue();

            if (block < currentBlock - depth) {
                toremove.add(entry.getKey());
                logger.trace(
                        "Clear outdated transaction, block.number: [{}] hash: [{}]",
                        block,
                        entry.getKey());
            }
        }

        removeTransactionList(toremove);

        if (timeout > 0) {
            this.removeObsoleteTransactions(timestampSeconds - timeout);
        }
    }

    @VisibleForTesting
    public synchronized void removeObsoleteTransactions(long timeSeconds) {
        List<Keccak256> toremove = new ArrayList<>();

        for (Map.Entry<Keccak256, Long> entry : transactionTimes.entrySet()) {
            long txtime = entry.getValue().longValue();

            if (txtime <= timeSeconds) {
                toremove.add(entry.getKey());
                logger.trace(
                        "Clear outdated transaction, hash: [{}]",
                        entry.getKey());
            }
        }

        removeTransactionList(toremove);
    }

    private void removeTransactionList(List<Keccak256> toremove) {
        for (Keccak256 key : toremove) {
            pendingTransactions.removeTransactionByHash(key);
            queuedTransactions.removeTransactionByHash(key);

            transactionBlocks.remove(key);
            transactionTimes.remove(key);
        }
    }

    @Override
    public synchronized void removeTransactions(List<Transaction> txs) {
        for (Transaction tx : txs) {
            Keccak256 khash = tx.getHash();
            pendingTransactions.removeTransactionByHash(khash);
            queuedTransactions.removeTransactionByHash(khash);

            logger.trace("Clear transaction, hash: [{}]", khash);
        }
    }

    @Override
    public synchronized List<Transaction> getPendingTransactions() {
        removeObsoleteTransactions(this.getCurrentBestBlockNumber(), this.outdatedThreshold, this.outdatedTimeout);
        return Collections.unmodifiableList(pendingTransactions.getTransactions());
    }

    @Override
    public synchronized List<Transaction> getQueuedTransactions() {
        removeObsoleteTransactions(this.getCurrentBestBlockNumber(), this.outdatedThreshold, this.outdatedTimeout);
        List<Transaction> ret = new ArrayList<>();
        ret.addAll(queuedTransactions.getTransactions());
        return ret;
    }

    private void addQueuedTransaction(Transaction tx) {
        this.queuedTransactions.addTransaction(tx);
    }

    private long getCurrentTimeInSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    private long getCurrentBestBlockNumber() {
        if (bestBlock == null) {
            return -1;
        }

        return bestBlock.getNumber();
    }

    private Block createFakePendingBlock(Block best) {
        // creating fake lightweight calculated block with no hashes calculations
        return blockFactory.newBlock(
                blockFactory.newHeader(
                        best.getHash().getBytes(), emptyUncleHashList, new byte[20],
                        new byte[32], EMPTY_TRIE_HASH, new byte[32],
                        new byte[32], best.getDifficulty().getBytes(), best.getNumber() + 1,
                        ByteUtil.longToBytesNoLeadZeroes(Long.MAX_VALUE), 0, best.getTimestamp() + 1,
                        new byte[0], Coin.ZERO, new byte[0], new byte[0], new byte[0], new byte[0],
                        ByteUtil.bigIntegerToBytes(BigInteger.ZERO), 0
                ),
                Collections.emptyList(),
                Collections.emptyList()
        );
    }

    private TransactionValidationResult shouldAcceptTx(Transaction tx, RepositorySnapshot currentRepository) {
        AccountState state = currentRepository.getAccountState(tx.getSender());
        return validator.isValid(tx, bestBlock, state);
    }

    /**
     * @param newTx a transaction to be added to the pending list (nonce = last pending nonce + 1)
     * @param currentRepository
     * @return whether the sender balance is enough to pay for all pending transactions + newTx
     */
    private boolean senderCanPayPendingTransactionsAndNewTx(
            Transaction newTx,
            RepositorySnapshot currentRepository) {
        List<Transaction> transactions = pendingTransactions.getTransactionsWithSender(newTx.getSender());

        Coin accumTxCost = Coin.ZERO;
        for (Transaction t : transactions) {
            accumTxCost = accumTxCost.add(getTxBaseCost(t));
        }

        Coin costWithNewTx = accumTxCost.add(getTxBaseCost(newTx));
        return costWithNewTx.compareTo(currentRepository.getBalance(newTx.getSender())) <= 0;
    }

    private Coin getTxBaseCost(Transaction tx) {
        Coin gasCost = tx.getValue();
        if (bestBlock == null || getTransactionCost(tx, bestBlock.getNumber()) > 0) {
            BigInteger gasLimit = new BigInteger(1, tx.getGasLimit());
            gasCost = gasCost.add(tx.getGasPrice().multiply(gasLimit));
        }

        return gasCost;
    }

    private long getTransactionCost(Transaction tx, long number) {
        return tx.transactionCost(
                config.getNetworkConstants(),
                config.getActivationConfig().forBlock(number)
        );
    }
}
