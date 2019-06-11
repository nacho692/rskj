/*
 * This file is part of RskJ
 * Copyright (C) 2019 RSK Labs Ltd.
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

package co.rsk.db;

import co.rsk.core.types.ints.Uint24;
import co.rsk.crypto.Keccak256;
import co.rsk.trie.MutableTrie;
import co.rsk.trie.Trie;
import org.ethereum.db.ByteArrayWrapper;
import org.ethereum.db.TrieKeyMapper;
import org.ethereum.vm.DataWord;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

public class MutableTrieCache implements MutableTrie {

    private MutableTrie trie;
    // We use a single cache to mark both changed elements and removed elements.
    // null value means the element has been removed.
    private final Map<ByteArrayWrapper, Map<ByteArrayWrapper, byte[]>> cache;

    private final Set<ByteArrayWrapper> deleteRecursiveCache;

    public MutableTrieCache(MutableTrie parentTrie) {
        trie = parentTrie;
        cache = new HashMap<>();
        deleteRecursiveCache = new HashSet<>();
    }

    @Override
    public Trie getTrie() {
        assertNoCache();
        return trie.getTrie();
    }

    @Override
    public Keccak256 getHash() {
        return trie.getHash();
    }

    @Override
    public byte[] get(byte[] key) {
        return internalGet(key, trie::get, Function.identity()).orElse(null);
    }

    private <T> Optional<T> internalGet(
            byte[] key,
            Function<byte[], T> trieRetriever,
            Function<byte[], T> cacheTransformer) {
        ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
        ByteArrayWrapper accountWrapper = getAccountWrapper(wrapper);
        byte[] cacheItem = cache.getOrDefault(accountWrapper, Collections.emptyMap()).get(wrapper);

        if (deleteRecursiveCache.contains(accountWrapper)){
            return Optional.empty();
        }

        return cacheItem != null ?
                Optional.ofNullable(cacheTransformer.apply(cacheItem)) :
                Optional.ofNullable(trieRetriever.apply(key));
    }

    public Iterator<DataWord> getStorageKeys(byte[] accountStorageKey) {
        ByteArrayWrapper accountWrapper = new ByteArrayWrapper(accountStorageKey);
        if (deleteRecursiveCache.contains(accountWrapper)) {
            return new ArrayList<DataWord>().iterator();
        }

        Iterator<DataWord> storageKeys = trie.getStorageKeys(accountStorageKey);
        Map<ByteArrayWrapper, byte[]> accountItems = new HashMap<>(cache.getOrDefault(accountWrapper, Collections.emptyMap()));
        if (accountItems.isEmpty()) {
            return storageKeys;
        }

        List<DataWord> keys = new ArrayList<>();
        while (storageKeys.hasNext()) {
            keys.add(storageKeys.next());
        }

        accountItems.forEach((key, value) -> {
            if (value != null) {
                keys.add(DataWord.valueOf(key.getData()));
            } else {
                keys.remove(DataWord.valueOf(key.getData()));
            }
        });

        return keys.iterator();
    }

    // This method returns a wrapper with the same content and size expected for a account key
    // when the key is from the same size than the original wrapper, it returns the same object
    private ByteArrayWrapper getAccountWrapper(ByteArrayWrapper originalWrapper) {
        byte[] key = originalWrapper.getData();
        int size = TrieKeyMapper.domainPrefix().length + TrieKeyMapper.ACCOUNT_KEY_SIZE + TrieKeyMapper.SECURE_KEY_SIZE;
        return key.length == size ? originalWrapper : new ByteArrayWrapper(Arrays.copyOf(key, size));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        put(new ByteArrayWrapper(key), value);
    }

    // This method optimizes cache-to-cache transfers
    @Override
    public void put(ByteArrayWrapper wrapper, byte[] value) {
        // If value==null, do we have the choice to either store it
        // in cache with null or in deleteCache. Here we have the choice to
        // to add it to cache with null value or to deleteCache.
        ByteArrayWrapper accountWrapper = getAccountWrapper(wrapper);
        Map<ByteArrayWrapper, byte[]> accountMap = cache.computeIfAbsent(accountWrapper, k -> new HashMap<>());
        accountMap.put(wrapper, value);
    }

    @Override
    public void put(String key, byte[] value) {
        byte[] keybytes = key.getBytes(StandardCharsets.UTF_8);
        put(keybytes, value);
    }

    ////////////////////////////////////////////////////////////////////////////////////
    // The semantic of implementations is special, and not the same of the MutableTrie
    // It is DELETE ON COMMIT, which means that changes are not applies until commit()
    // is called, and changes are applied last.
    ////////////////////////////////////////////////////////////////////////////////////
    @Override
    public void deleteRecursive(byte[] key) {
        // Can there be wrongly unhandled interactions interactions between put() and deleteRecurse()
        // In theory, yes. In practice, never.
        // Suppose that a contract X calls a contract S.
        // Contract S calls itself with CALL.
        // Contract S suicides with SUICIDE opcode.
        // This causes a return to prev contract.
        // But the SUICIDE DOES NOT cause the storage keys to be removed YET.
        // Now parent contract S is still running, and it then can create a new storage cell
        // with SSTORE. This will be stored in the cache as a put(). The cache later receives a
        // deleteRecursive, BUT NEVER IN THE OTHER order.
        // See TransactionExecutor.finalization(), when it iterates the list with getDeleteAccounts().forEach()
        ByteArrayWrapper wrap = new ByteArrayWrapper(key);
        deleteRecursiveCache.add(wrap);
    }

    @Override
    public void commit() {
        // all cached items must be transferred to parent
        // some items will represent deletions (null values)
        cache.forEach((accountKey, accountData) -> {
            if (deleteRecursiveCache.contains(accountKey)) {
                this.trie.deleteRecursive(accountKey.getData());
            } else {
                accountData.forEach((realKey, value) -> this.trie.put(realKey, value));
            }
        });

        deleteRecursiveCache.clear();
        cache.clear();
    }

    @Override
    public void save() {
        commit();
        trie.save();
    }

    @Override
    public void flush() {
        trie.flush();
    }

    @Override
    public void rollback() {
        cache.clear();
        deleteRecursiveCache.clear();
    }

    @Override
    public Set<ByteArrayWrapper> collectKeys(int size) {
        Set<ByteArrayWrapper> parentSet = trie.collectKeys(size);

        // all cached items to be transferred to parent
        cache.forEach((accountKey, account) ->
              account.forEach((realKey, value) -> {
                  if (size == Integer.MAX_VALUE || realKey.getData().length == size) {
                      if (this.get(realKey.getData()) == null) {
                          parentSet.remove(realKey);
                      } else {
                          parentSet.add(realKey);
                      }
                  }
              }));
        return parentSet;
    }

    private void assertNoCache() {
        if (cache.size() != 0) {
            throw new IllegalStateException();
        }

        if (deleteRecursiveCache.size() != 0) {
            throw new IllegalStateException();
        }
    }

    @Override
    public MutableTrie getSnapshotTo(Keccak256 hash) {
        assertNoCache();
        return new MutableTrieCache(trie.getSnapshotTo(hash));
    }

    @Override
    public boolean hasStore() {
        return trie.hasStore();
    }

    @Override
    public Uint24 getValueLength(byte[] key) {
        return internalGet(key, trie::getValueLength, cachedBytes -> new Uint24(cachedBytes.length)).orElse(Uint24.ZERO);
    }

}
