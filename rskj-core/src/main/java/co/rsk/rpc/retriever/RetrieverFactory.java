package co.rsk.rpc.retriever;

import co.rsk.db.RepositoryLocator;
import org.ethereum.core.Block;
import org.ethereum.core.Blockchain;
import org.ethereum.core.TransactionPool;

import static org.ethereum.rpc.TypeConverter.stringHexToBigInteger;
import static org.ethereum.rpc.exception.RskJsonRpcRequestException.invalidParamError;

public class RetrieverFactory {
    private final TransactionPool transactionPool;
    private final Blockchain blockchain;
    private final RepositoryLocator locator;

    public RetrieverFactory(TransactionPool transactionPool, Blockchain blockchain, RepositoryLocator locator) {

        this.transactionPool = transactionPool;
        this.blockchain = blockchain;
        this.locator = locator;
    }

    public BlockInformationRetriever getRetriever(String bnOrId) {
        if ("pending".equals(bnOrId)) {
            return new PendingRetriever(transactionPool);
        }
        Block block;
        if ("latest".equals(bnOrId)) {
            block = blockchain.getBestBlock();
        } else if ("earliest".equals(bnOrId)) {
            block = blockchain.getBlockByNumber(0);
        } else {
            try {
                long blockNumber = stringHexToBigInteger(bnOrId).longValue();
                block = this.blockchain.getBlockByNumber(blockNumber);
            } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
                throw invalidParamError("invalid blocknumber " + bnOrId);
            }
        }
        if (block != null) {
            return new BlockRetriever(block, locator);
        }

        return new BlockNotFoundRetriever();
    }
}