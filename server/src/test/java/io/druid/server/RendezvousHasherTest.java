/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

public class RendezvousHasherTest
{
  @Test
  public void testBasic() throws Exception
  {
    RendezvousHasher hasher = new RendezvousHasher();
    Map<String, String> nodes = new HashMap<>();
    nodes.put("localhost:1", "localhost:1");
    nodes.put("localhost:2", "localhost:2");
    nodes.put("localhost:3", "localhost:3");
    nodes.put("localhost:4", "localhost:4");
    nodes.put("localhost:5", "localhost:5");

    Map<String, Integer> nodeCounts = new HashMap<>();
    nodeCounts.put("localhost:1", 0);
    nodeCounts.put("localhost:2", 0);
    nodeCounts.put("localhost:3", 0);
    nodeCounts.put("localhost:4", 0);
    nodeCounts.put("localhost:5", 0);

    Map<String, String> uuidServerMap = new HashMap<>();

    for (int i = 0; i < 10000; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.chooseNode(nodes, objectId.toString(), RendezvousHasher.STRING_FUNNEL);
      int count = nodeCounts.get(targetServer);
      nodeCounts.put(targetServer, count + 1);
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    // check that the same UUIDs hash to the same servers on subsequent hashStr() calls
    for (int i = 0; i < 2; i++) {
      for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
        String targetServer = hasher.chooseNode(nodes, entry.getKey(), RendezvousHasher.STRING_FUNNEL);
        Assert.assertEquals(entry.getValue(), targetServer);
      }
    }
  }

  @Test
  public void testAddNode() throws Exception
  {
    int numIterations = 10000;

    RendezvousHasher hasher = new RendezvousHasher();
    Map<String, String> nodes = new HashMap<>();
    nodes.put("localhost:1", "localhost:1");
    nodes.put("localhost:2", "localhost:2");
    nodes.put("localhost:3", "localhost:3");
    nodes.put("localhost:4", "localhost:4");
    nodes.put("localhost:5", "localhost:5");

    Map<String, String> uuidServerMap = new HashMap<>();

    for (int i = 0; i < numIterations; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.chooseNode(nodes, objectId.toString(), RendezvousHasher.STRING_FUNNEL);
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    nodes.put("localhost:6", "localhost:6");

    int same = 0;
    int diff = 0;
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String targetServer = hasher.chooseNode(nodes, entry.getKey(), RendezvousHasher.STRING_FUNNEL);
      if (entry.getValue().equals(targetServer)) {
        same += 1;
      } else {
        diff += 1;
      }
    }
    System.out.println(StringUtils.format("testAddNode Total: %s, Same: %s, Diff: %s", numIterations, same, diff));
  }

  @Test
  public void testRemoveNode() throws Exception
  {
    int numIterations = 10000;

    RendezvousHasher hasher = new RendezvousHasher();
    Map<String, String> nodes = new HashMap<>();
    nodes.put("localhost:1", "localhost:1");
    nodes.put("localhost:2", "localhost:2");
    nodes.put("localhost:3", "localhost:3");
    nodes.put("localhost:4", "localhost:4");
    nodes.put("localhost:5", "localhost:5");

    Map<String, String> uuidServerMap = new HashMap<>();

    for (int i = 0; i < numIterations; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.chooseNode(nodes, objectId.toString(), RendezvousHasher.STRING_FUNNEL);
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    nodes.remove("localhost:3");

    int same = 0;
    int diff = 0;
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String targetServer = hasher.chooseNode(nodes, entry.getKey(), RendezvousHasher.STRING_FUNNEL);
      if (entry.getValue().equals(targetServer)) {
        same += 1;
      } else {
        diff += 1;
      }
    }
    System.out.println(StringUtils.format("testRemoveNode Total: %s, Same: %s, Diff: %s", numIterations, same, diff));
  }

  @Test
  public void testInconsistentView1() throws Exception
  {
    Map<String, String> nodes = new HashMap<>();
    nodes.put("localhost:1", "localhost:1");
    nodes.put("localhost:2", "localhost:2");
    nodes.put("localhost:3", "localhost:3");
    nodes.put("localhost:4", "localhost:4");
    nodes.put("localhost:5", "localhost:5");

    Map<String, String> nodes2 = new HashMap<>();
    nodes2.put("localhost:1", "localhost:1");
    nodes2.put("localhost:3", "localhost:3");
    nodes2.put("localhost:4", "localhost:4");
    nodes2.put("localhost:5", "localhost:5");

    testInconsistentViewHelper("testInconsistentView1", nodes, nodes2);
  }

  @Test
  public void testInconsistentView2() throws Exception
  {
    Map<String, String> nodes = new HashMap<>();
    nodes.put("localhost:1", "localhost:1");
    nodes.put("localhost:3", "localhost:3");
    nodes.put("localhost:4", "localhost:4");
    nodes.put("localhost:5", "localhost:5");

    Map<String, String> nodes2 = new HashMap<>();
    nodes2.put("localhost:1", "localhost:1");
    nodes2.put("localhost:2", "localhost:2");
    nodes2.put("localhost:4", "localhost:4");
    nodes2.put("localhost:5", "localhost:5");

    testInconsistentViewHelper("testInconsistentView2", nodes, nodes2);
  }

  @Test
  public void testInconsistentView3() throws Exception
  {
    Map<String, String> nodes = new HashMap<>();
    nodes.put("localhost:3", "localhost:3");
    nodes.put("localhost:4", "localhost:4");
    nodes.put("localhost:5", "localhost:5");

    Map<String, String> nodes2 = new HashMap<>();
    nodes2.put("localhost:1", "localhost:1");
    nodes2.put("localhost:4", "localhost:4");
    nodes2.put("localhost:5", "localhost:5");

    testInconsistentViewHelper("testInconsistentView3", nodes, nodes2);
  }

  @Test
  public void testInconsistentView4() throws Exception
  {
    Map<String, String> nodes = new HashMap<>();
    nodes.put("localhost:2", "localhost:2");
    nodes.put("localhost:5", "localhost:5");

    Map<String, String> nodes2 = new HashMap<>();
    nodes2.put("localhost:1", "localhost:1");
    nodes2.put("localhost:4", "localhost:4");
    nodes2.put("localhost:5", "localhost:5");

    testInconsistentViewHelper("testInconsistentView4", nodes, nodes2);
  }

  public void testInconsistentViewHelper(
      String testName,
      Map<String, String> nodes,
      Map<String, String> nodes2
  ) throws Exception
  {
    int numIterations = 100000;

    RendezvousHasher hasher = new RendezvousHasher();
    Map<String, String> uuidServerMap = new HashMap<>();
    for (int i = 0; i < numIterations; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.chooseNode(nodes, objectId.toString(), RendezvousHasher.STRING_FUNNEL);
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    RendezvousHasher hasher2 = new RendezvousHasher();
    Map<String, String> uuidServerMap2 = new HashMap<>();
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String targetServer = hasher2.chooseNode(nodes2, entry.getKey(), RendezvousHasher.STRING_FUNNEL);
      uuidServerMap2.put(entry.getKey(), targetServer);
    }

    int same = 0;
    int diff = 0;
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String otherServer = uuidServerMap2.get(entry.getKey());
      if (entry.getValue().equals(otherServer)) {
        same += 1;
      } else {
        diff += 1;
        //System.out.println(StringUtils.format("%s moved to %s.", entry.getValue(), otherServer));
      }
    }
    System.out.println(StringUtils.format("%s Total: %s, Same: %s, Diff: %s", testName, numIterations, same, diff));
  }
}
