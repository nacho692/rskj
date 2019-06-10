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

package co.rsk.peg;

import co.rsk.bitcoinj.core.BtcBlock;
import co.rsk.bitcoinj.core.NetworkParameters;
import co.rsk.bitcoinj.core.Sha256Hash;
import co.rsk.bitcoinj.core.StoredBlock;
import co.rsk.bitcoinj.params.RegTestParams;
import co.rsk.bitcoinj.store.BlockStoreException;
import co.rsk.config.BridgeConstants;
import co.rsk.config.BridgeRegTestConstants;
import co.rsk.db.MutableTrieCache;
import co.rsk.db.MutableTrieImpl;
import co.rsk.trie.Trie;
import org.apache.commons.lang3.tuple.Triple;
import org.ethereum.core.Repository;
import org.ethereum.db.MutableRepository;
import org.ethereum.vm.PrecompiledContracts;
import org.junit.Test;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class RepositoryBtcBlockStoreWithCacheTest {

    @Test
    public void getChainHead_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);

        assertEquals(networkParameters.getGenesisBlock(), btcBlockStore.getChainHead().getHeader());
    }

    @Test
    public void getParams_Test() {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);

        assertEquals(networkParameters, btcBlockStore.getParams());
    }

    @Test
    public void setChainHead_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);


        BtcBlock parent = networkParameters.getGenesisBlock();
        BtcBlock firstBlockHeader = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock firstStoredBlock = new StoredBlock(firstBlockHeader, new BigInteger("0"), 1);
        Sha256Hash firstBlockHash = firstBlockHeader.getHash();

        assertEquals(parent.getHash(), btcBlockStore.getChainHead().getHeader().getHash());

        //There should always be a put before a setChainHead
        btcBlockStore.put(firstStoredBlock);
        btcBlockStore.setChainHead(firstStoredBlock);

        assertEquals(firstStoredBlock, btcBlockStore.get(firstBlockHash));
        assertEquals(firstStoredBlock, btcBlockStore.getChainHead());
        assertEquals(firstStoredBlock, btcBlockStore.getFromCache(firstBlockHash));

    }


    @Test
    public void put_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);


        BtcBlock genesis = networkParameters.getGenesisBlock();
        BtcBlock parent = genesis;
        BtcBlock firstBlockHeader = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock firstStoredBlock = new StoredBlock(firstBlockHeader, new BigInteger("0"), 1);
        Sha256Hash firstBlockHash = firstBlockHeader.getHash();

        btcBlockStore.put(firstStoredBlock);

        assertEquals(firstStoredBlock, btcBlockStore.get(firstBlockHash));
        assertEquals(firstStoredBlock, btcBlockStore.getFromCache(firstBlockHash));
        assertEquals(genesis, btcBlockStore.getChainHead().getHeader());

    }

    @Test
    public void ifCacheNullAlwaysGoToDisk_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = new RepositoryBtcBlockStoreWithCache(networkParameters, track, null, PrecompiledContracts.BRIDGE_ADDR);

        BtcBlock genesis = networkParameters.getGenesisBlock();
        assertEquals(genesis, btcBlockStore.getChainHead().getHeader());

        BtcBlock parent = genesis;
        BtcBlock firstBlockHeader = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock firstStoredBlock = new StoredBlock(firstBlockHeader, new BigInteger("0"), 1);
        Sha256Hash firstBlockHash = firstBlockHeader.getHash();

        btcBlockStore.put(firstStoredBlock);
        btcBlockStore.setChainHead(firstStoredBlock);

        assertEquals(firstStoredBlock, btcBlockStore.get(firstBlockHash));
        assertNull(btcBlockStore.getFromCache(firstBlockHash));

        assertEquals(firstStoredBlock, btcBlockStore.getChainHead());
        assertEquals(genesis, btcBlockStore.getStoredBlockAtMainChainDepth(1).getHeader());
        assertEquals(genesis, btcBlockStore.getStoredBlockAtMainChainHeight(0).getHeader());
    }

    @Test
    public void put_oldBlockShouldNotGoToCache_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);


        BtcBlock genesis = networkParameters.getGenesisBlock();

        BtcBlock parent = genesis;
        BtcBlock blockHeader1 = new BtcBlock(networkParameters, 2l, genesis.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock1 = new StoredBlock(blockHeader1, new BigInteger("0"), 6000);
        btcBlockStore.put(storedBlock1);
        btcBlockStore.setChainHead(storedBlock1);

        parent = blockHeader1;
        BtcBlock blockHeader2 = new BtcBlock(networkParameters, 2l, genesis.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock2 = new StoredBlock(blockHeader2, new BigInteger("0"), 1);
        btcBlockStore.put(storedBlock2);


        assertEquals(storedBlock1, btcBlockStore.get(storedBlock1.getHeader().getHash()));
        assertEquals(storedBlock1, btcBlockStore.getFromCache(storedBlock1.getHeader().getHash()));
        assertEquals(storedBlock2, btcBlockStore.get(storedBlock2.getHeader().getHash()));
        assertNull(btcBlockStore.getFromCache(storedBlock2.getHeader().getHash()));
        assertEquals(storedBlock1, btcBlockStore.getChainHead());
    }

    @Test
    public void cacheLivesAcrossInstances_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);

        BtcBlock genesis = networkParameters.getGenesisBlock();
        BtcBlock firstBlockHeader = mock(BtcBlock.class);
        Sha256Hash firstBlockHash = Sha256Hash.wrap("0000000000000000000000000000000000000000000000000000000000000001");
        when(firstBlockHeader.getHash()).thenReturn(firstBlockHash);

        StoredBlock firstStoredBlock = mock(StoredBlock.class);
        when(firstStoredBlock.getHeader()).thenReturn(firstBlockHeader);
        when(firstStoredBlock.getHeight()).thenReturn(50);
        btcBlockStore.put(firstStoredBlock);

        //Cache should have the genesis block and the one we just added
        assertNotNull(btcBlockStore.getFromCache(genesis.getHash()));
        assertEquals(firstStoredBlock, btcBlockStore.getFromCache(firstBlockHash));

        BtcBlockStoreWithCache btcBlockStore2 = factory.newInstance(repository.startTracking());
        BtcBlock secondBlockHeader = mock(BtcBlock.class);
        Sha256Hash secondBlockHash = Sha256Hash.wrap("0000000000000000000000000000000000000000000000000000000000000002");
        when(secondBlockHeader.getHash()).thenReturn(secondBlockHash);
        when(secondBlockHeader.getPrevBlockHash()).thenReturn(firstBlockHash);

        StoredBlock secondStoredBlock = mock(StoredBlock.class);
        when(secondStoredBlock.getHeader()).thenReturn(secondBlockHeader);
        when(secondStoredBlock.getHeight()).thenReturn(50);
        btcBlockStore2.put(secondStoredBlock);

        assertEquals(firstStoredBlock, btcBlockStore2.getFromCache(firstBlockHash));
        assertEquals(secondStoredBlock, btcBlockStore2.getFromCache(secondBlockHash));
    }


    @Test
    public void getStoredBlockAtMainChainDepth_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);

        BtcBlock genesis = networkParameters.getGenesisBlock();

        BtcBlock parent = genesis;
        BtcBlock blockHeader1 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock1 = new StoredBlock(blockHeader1, new BigInteger("0"), 1);
        btcBlockStore.put(storedBlock1);

        parent = blockHeader1;
        BtcBlock blockHeader2 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock2 = new StoredBlock(blockHeader2, new BigInteger("0"), 2);
        btcBlockStore.put(storedBlock2);

        parent = blockHeader2;
        BtcBlock blockHeader3 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock3 = new StoredBlock(blockHeader3, new BigInteger("0"), 3);
        btcBlockStore.put(storedBlock3);

        parent = blockHeader3;
        BtcBlock blockHeader4 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock4 = new StoredBlock(blockHeader4, new BigInteger("0"), 4);
        btcBlockStore.put(storedBlock4);

        btcBlockStore.setChainHead(storedBlock4);

        assertEquals(storedBlock3, btcBlockStore.getStoredBlockAtMainChainDepth(1));
        assertEquals(storedBlock2, btcBlockStore.getStoredBlockAtMainChainDepth(2));
        assertEquals(storedBlock1, btcBlockStore.getStoredBlockAtMainChainDepth(3));
        assertEquals(genesis, btcBlockStore.getStoredBlockAtMainChainDepth(4).getHeader());
        assertEquals(storedBlock4, btcBlockStore.getChainHead());
    }

    @Test(expected = BlockStoreException.class)
    public void getStoredBlockAtMainChainDepth_Error_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);

        BtcBlock genesis = networkParameters.getGenesisBlock();

        BtcBlock parent = genesis;
        BtcBlock blockHeader1 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock1 = new StoredBlock(blockHeader1, new BigInteger("0"), 2);
        btcBlockStore.put(storedBlock1);

        parent = blockHeader1;
        BtcBlock blockHeader2 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock2 = new StoredBlock(blockHeader2, new BigInteger("0"), 2);
        btcBlockStore.put(storedBlock2);

        btcBlockStore.setChainHead(storedBlock2);

        btcBlockStore.getStoredBlockAtMainChainDepth(1);
    }

    @Test
    public void getStoredBlockAtMainChainHeight_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);

        BtcBlock genesis = networkParameters.getGenesisBlock();

        BtcBlock parent = genesis;
        BtcBlock blockHeader1 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock1 = new StoredBlock(blockHeader1, new BigInteger("0"), 1);
        btcBlockStore.put(storedBlock1);

        parent = blockHeader1;
        BtcBlock blockHeader2 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock2 = new StoredBlock(blockHeader2, new BigInteger("0"), 2);
        btcBlockStore.put(storedBlock2);

        parent = blockHeader2;
        BtcBlock blockHeader3 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock3 = new StoredBlock(blockHeader3, new BigInteger("0"), 3);
        btcBlockStore.put(storedBlock3);

        parent = blockHeader3;
        BtcBlock blockHeader4 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock4 = new StoredBlock(blockHeader4, new BigInteger("0"), 4);
        btcBlockStore.put(storedBlock4);

        btcBlockStore.setChainHead(storedBlock4);

        assertEquals(storedBlock4, btcBlockStore.getStoredBlockAtMainChainHeight(4));
        assertEquals(storedBlock3, btcBlockStore.getStoredBlockAtMainChainHeight(3));
        assertEquals(storedBlock2, btcBlockStore.getStoredBlockAtMainChainHeight(2));
        assertEquals(storedBlock1, btcBlockStore.getStoredBlockAtMainChainHeight(1));
        assertEquals(genesis, btcBlockStore.getStoredBlockAtMainChainHeight(0).getHeader());
        assertEquals(storedBlock4, btcBlockStore.getChainHead());
    }


    @Test(expected = BlockStoreException.class)
    public void getStoredBlockAtMainChainHeight_Error_Test() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        Repository track = repository.startTracking();
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(track);

        BtcBlock genesis = networkParameters.getGenesisBlock();

        BtcBlock parent = genesis;
        BtcBlock blockHeader1 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock1 = new StoredBlock(blockHeader1, new BigInteger("0"), 2);
        btcBlockStore.put(storedBlock1);

        parent = blockHeader1;
        BtcBlock blockHeader2 = new BtcBlock(networkParameters, 2l, parent.getHash(), Sha256Hash.ZERO_HASH, parent.getTimeSeconds()+1, parent.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock storedBlock2 = new StoredBlock(blockHeader2, new BigInteger("0"), 2);
        btcBlockStore.put(storedBlock2);

        btcBlockStore.setChainHead(storedBlock2);

        btcBlockStore.getStoredBlockAtMainChainHeight(1);
    }

    @Test
    public void RepoIsNotSharedAcrossTracksTest() throws BlockStoreException {
        NetworkParameters networkParameters = BridgeRegTestConstants.getInstance().getBtcParams();
        RepositoryBtcBlockStoreWithCache.Factory factory = new RepositoryBtcBlockStoreWithCache.Factory(networkParameters);
        Repository repository =  new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        BtcBlockStoreWithCache btcBlockStore = factory.newInstance(repository.startTracking());
        BtcBlock genesisBlock = networkParameters.getGenesisBlock();

        BtcBlock firstBlockHeader = new BtcBlock(networkParameters, 2l, genesisBlock.getHash(), Sha256Hash.ZERO_HASH, genesisBlock.getTimeSeconds()+1, genesisBlock.getDifficultyTarget(), 0, new ArrayList<>());
        StoredBlock firstStoredBlock = new StoredBlock(firstBlockHeader, new BigInteger("0"), 1);
        Sha256Hash firstBlockHash = firstBlockHeader.getHash();

        btcBlockStore.put(firstStoredBlock);

        //Cache should have the genesis block and the one we just added
        assertNotNull(btcBlockStore.getFromCache(genesisBlock.getHash()));
        assertEquals(firstStoredBlock, btcBlockStore.getFromCache(firstBlockHash));


        BtcBlockStoreWithCache btcBlockStore2 = factory.newInstance(repository.startTracking());
        BtcBlock secondBlockHeader = new BtcBlock(networkParameters, 2l, genesisBlock.getHash(), Sha256Hash.ZERO_HASH, genesisBlock.getTimeSeconds()+1, genesisBlock.getDifficultyTarget(), 1, new ArrayList<>());
        StoredBlock secondStoredBlock = new StoredBlock(secondBlockHeader, new BigInteger("0"), 1);
        Sha256Hash secondBlockHash = secondBlockHeader.getHash();
        btcBlockStore2.put(secondStoredBlock);

        //Trie should have only te second one
        assertNull(btcBlockStore2.get(firstBlockHash));
        assertEquals(secondStoredBlock, btcBlockStore2.get(secondBlockHash));

        //Cache has both of them
        assertEquals(firstStoredBlock, btcBlockStore2.getFromCache(firstBlockHash));
        assertEquals(secondStoredBlock, btcBlockStore2.getFromCache(secondBlockHash));
    }

    @Test
    public void checkDifferentInstancesWithSameRepoHaveSameContentTest() throws Exception {
//        This Is how I produced RepositoryBlockStore_data.ser. I had a bitcoind in regtest with 613 blocks + genesis block
//        NetworkParameters params = RegTestParams.get();
//        Context context = new Context(params);
//        Wallet wallet = new Wallet(context);
//        BlockStore store = new SPVBlockStore(params, new File("spvBlockstore"));
//        AbstractBlockChain chain = new BlockChain(context, wallet, store);
//        PeerGroup peerGroup = new PeerGroup(context, chain);
//        peerGroup.start();
//        final DownloadProgressTracker listener = new DownloadProgressTracker();
//        peerGroup.startBlockChainDownload(listener);
//        listener.await();
//        peerGroup.stop();
//        StoredBlock storedBlock = chain.getChainHead();
//        FileOutputStream fos = new FileOutputStream("RepositoryBlockStore_data.ser");
//        ObjectOutputStream oos = new ObjectOutputStream(fos);
//        for (int i = 0; i < 614; i++) {
//            Triple<byte[], BigInteger , Integer> tripleStoredBlock = new ImmutableTriple<>(storedBlock.getHeader().bitcoinSerialize(), storedBlock.getChainWork(), storedBlock.getHeight());
//            oos.writeObject(tripleStoredBlock);
//            storedBlock = store.get(storedBlock.getHeader().getPrevBlockHash());
//        }
//        oos.close();

        // Read original store
        InputStream fileInputStream = ClassLoader.getSystemResourceAsStream("peg/RepositoryBlockStore_data.ser");
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        Repository repository = new MutableRepository(new MutableTrieCache(new MutableTrieImpl(new Trie())));
        BridgeConstants bridgeConstants = BridgeRegTestConstants.getInstance();
        BtcBlockStoreWithCache.Factory btcBlockStoreFactory = new RepositoryBtcBlockStoreWithCache.Factory(bridgeConstants.getBtcParams());
        BtcBlockStoreWithCache store = btcBlockStoreFactory.newInstance(repository);
        for (int i = 0; i < 614; i++) {
            Triple<byte[], BigInteger , Integer> tripleStoredBlock = (Triple<byte[], BigInteger , Integer>) objectInputStream.readObject();
            BtcBlock header = RegTestParams.get().getDefaultSerializer().makeBlock(tripleStoredBlock.getLeft());
            StoredBlock storedBlock = new StoredBlock(header, tripleStoredBlock.getMiddle(), tripleStoredBlock.getRight());
            if (i==0) {
                store.setChainHead(storedBlock);
            }
            store.put(storedBlock);
        }

        // Create a new instance of the store
        BtcBlockStoreWithCache store2 =btcBlockStoreFactory.newInstance(repository);

        // Check a specific block that used to fail when we had a bug
        assertEquals(store.get(Sha256Hash.wrap("373941fe83961cf70e181e468abc5f9f7cc440c711c3d06948fa66f3912ed27a")),
                store2.get(Sha256Hash.wrap("373941fe83961cf70e181e468abc5f9f7cc440c711c3d06948fa66f3912ed27a")));

        //Check new instance content is identical to the original one
        StoredBlock storedBlock = store.getChainHead();
        StoredBlock storedBlock2 = store2.getChainHead();
        int headHeight = storedBlock.getHeight();
        for (int i = 0; i < headHeight; i++) {
            assertNotNull(storedBlock);
            assertEquals(storedBlock, storedBlock2);
            Sha256Hash prevBlockHash = storedBlock.getHeader().getPrevBlockHash();
            storedBlock = store.get(prevBlockHash);
            storedBlock2 = store2.get(prevBlockHash);
        }
    }
}
