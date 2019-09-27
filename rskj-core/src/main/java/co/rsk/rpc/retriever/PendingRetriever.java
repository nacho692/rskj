package co.rsk.rpc.retriever;

import co.rsk.core.bc.AccountInformationProvider;
import org.ethereum.core.Block;
import org.ethereum.core.Transaction;
import org.ethereum.core.TransactionPool;

import java.util.List;
import java.util.Optional;

public class PendingRetriever implements BlockInformationRetriever {

    private final TransactionPool transactionPool;

    public PendingRetriever(TransactionPool transactionPool) {
        this.transactionPool = transactionPool;
    }

    @Override
    public Optional<List<Transaction>> getTransactions() {
        return Optional.of(transactionPool.getPendingTransactions());    }

    @Override
    public Optional<Block> getBlock() {
        return Optional.empty();
    }

    @Override
    public Optional<AccountInformationProvider> getAccountInformationProvider() {
        return Optional.of(transactionPool.getPendingState());
    }
}