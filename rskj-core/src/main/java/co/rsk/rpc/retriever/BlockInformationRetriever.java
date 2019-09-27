package co.rsk.rpc.retriever;

import co.rsk.core.bc.AccountInformationProvider;
import co.rsk.db.RepositorySnapshot;
import org.ethereum.core.Block;
import org.ethereum.core.Transaction;

import java.util.List;
import java.util.Optional;

public interface BlockInformationRetriever {
    Optional<List<Transaction>> getTransactions();
    Optional<Block> getBlock();
    Optional<AccountInformationProvider> getAccountInformationProvider();
}
