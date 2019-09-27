package co.rsk.rpc.retriever;

import co.rsk.core.bc.AccountInformationProvider;
import org.ethereum.core.Block;
import org.ethereum.core.Transaction;
import org.ethereum.rpc.exception.RskJsonRpcRequestException;

import java.util.List;
import java.util.Optional;

public class BlockNotFoundRetriever implements BlockInformationRetriever {

    public BlockNotFoundRetriever() {
    }

    @Override
    public Optional<List<Transaction>> getTransactions() {
        return Optional.empty();
    }

    @Override
    public Optional<Block> getBlock() {
        return Optional.empty();
    }

    @Override
    public Optional<AccountInformationProvider> getAccountInformationProvider() {
        return Optional.empty();
    }
}