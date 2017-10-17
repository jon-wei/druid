package io.druid.server;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;

import java.util.List;
import java.util.Map;

// Distributes objects across a set of node keys using rendezvous hashing
// See https://en.wikipedia.org/wiki/Rendezvous_hashing
public class RendezvousHasher
{
  private static final HashFunction HASH_FN = Hashing.murmur3_128(9999);

  public static Funnel STRING_FUNNEL = Funnels.stringFunnel(Charsets.UTF_8);

  public <NodeType, KeyType> NodeType chooseNode(Map<String, NodeType> nodeMap, KeyType key, Funnel<KeyType> funnel)
  {
    if (nodeMap.isEmpty()) {
      return null;
    }

    Long2ObjectRBTreeMap<NodeType> weights = new Long2ObjectRBTreeMap<>();
    weights.defaultReturnValue(null);

    for (Map.Entry<String, NodeType> node : nodeMap.entrySet()) {
      HashCode keyHash = HASH_FN.hashObject(key, funnel);
      HashCode nodeHash = HASH_FN.hashObject(node.getKey(), STRING_FUNNEL);
      List<HashCode> hashes = Lists.newArrayList(nodeHash, keyHash);
      HashCode combinedHash = Hashing.combineOrdered(hashes);
      weights.put(combinedHash.asLong(), node.getValue());
    }

    return weights.get(weights.lastLongKey());
  }

}
