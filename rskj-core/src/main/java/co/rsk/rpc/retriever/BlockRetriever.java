package co.rsk.rpc.retriever;

import co.rsk.core.bc.AccountInformationProvider;
import co.rsk.db.RepositoryLocator;
import org.ethereum.core.Block;
import org.ethereum.core.Transaction;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.ethereum.rpc.exception.RskJsonRpcRequestException.stateNotFound;

public class BlockRetriever implements BlockInformationRetriever {

    private final Block block;
    private final RepositoryLocator repositoryLocator;

    public BlockRetriever(Block block, RepositoryLocator repositoryLocator) {
        this.block = block;
        this.repositoryLocator = repositoryLocator;
    }

    @Override
    public Optional<List<Transaction>> getTransactions() {
        return Optional.of(block.getTransactionsList());
    }

    @Override
    public Optional<Block> getBlock() {
        return Optional.of(block);
    }

    @Override
    public Optional<AccountInformationProvider> getAccountInformationProvider() {
        return repositoryLocator.findSnapshotAt(block.getHeader()).map(Function.identity());
    }
}