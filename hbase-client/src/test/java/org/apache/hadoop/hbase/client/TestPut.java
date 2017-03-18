/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category({ SmallTests.class, ClientTests.class })
public class TestPut {
  @Test
  public void testCopyConstructor() {
    Put origin = new Put(Bytes.toBytes("ROW-01"));
    byte[] family = Bytes.toBytes("CF-01");
    byte[] qualifier = Bytes.toBytes("Q-01");

    origin.addColumn(family, qualifier, Bytes.toBytes("V-01"));
    Put clone = new Put(origin);

    assertEquals(origin.getCellList(family), clone.getCellList(family));
    origin.addColumn(family, qualifier, Bytes.toBytes("V-02"));

    //They should have different cell lists
    assertNotEquals(origin.getCellList(family), clone.getCellList(family));
  }

  // HBASE-14881
  @Test
  public void testRowIsImmutableOrNot() {
    byte[] rowKey = Bytes.toBytes("immutable");

    // Test when row key is immutable
    Put putRowIsImmutable = new Put(rowKey, true);
    assertTrue(rowKey == putRowIsImmutable.getRow());  // No local copy is made

    // Test when row key is not immutable
    Put putRowIsNotImmutable = new Put(rowKey, 1000L, false);
    assertTrue(rowKey != putRowIsNotImmutable.getRow());  // A local copy is made
  }

  // HBASE-14882
  @Test
  public void testAddImmutable() {
    byte[] row        = Bytes.toBytes("immutable-row");
    byte[] family     = Bytes.toBytes("immutable-family");

    byte[] qualifier0 = Bytes.toBytes("immutable-qualifier-0");
    byte[] value0     = Bytes.toBytes("immutable-value-0");

    byte[] qualifier1 = Bytes.toBytes("immutable-qualifier-1");
    byte[] value1     = Bytes.toBytes("immutable-value-1");
    long   ts1        = 5000L;

    Put put = new Put(row, true);  // "true" indicates that the input row is immutable
    put.addImmutable(family, qualifier0, value0);
    put.addImmutable(family, qualifier1, ts1, value1);

    // Verify the cell of family:qualifier0
    Cell cell0 = put.get(family, qualifier0).get(0);

    // Verify no local copy is made for family, qualifier or value
    assertTrue(cell0.getFamilyArray()    == family);
    assertTrue(cell0.getQualifierArray() == qualifier0);
    assertTrue(cell0.getValueArray()     == value0);

    // Verify timestamp
    assertTrue(cell0.getTimestamp()      == put.getTimeStamp());

    // Verify the cell of family:qualifier1
    Cell cell1 = put.get(family, qualifier1).get(0);

    // Verify no local copy is made for family, qualifier or value
    assertTrue(cell1.getFamilyArray()    == family);
    assertTrue(cell1.getQualifierArray() == qualifier1);
    assertTrue(cell1.getValueArray()     == value1);

    // Verify timestamp
    assertTrue(cell1.getTimestamp()      == ts1);
  }

  @Test
  public void testAddColumn() {
    Put put = new Put(Bytes.toBytes("row"));
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");

    Put result = put.addColumn(family, qualifier, value);

    assertEquals(1, result.familyMap.size());
    assertTrue(result.has(family, qualifier, value));
  }

  @Test
  public void testAddColumnWithTimestamp() {
    Put put = new Put(Bytes.toBytes("row"));
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");
    long timestamp = System.currentTimeMillis();

    Put result = put.addColumn(family, qualifier, timestamp, value);

    assertEquals(1, result.familyMap.size());
    assertTrue(result.has(family, qualifier, timestamp, value));
  }

  @Test
  public void testAddColumnByteBuffer() {
    Put put = new Put(Bytes.toBytes("row"));
    byte[] family = Bytes.toBytes("family");
    ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes("qualifier"));
    long timestamp = System.currentTimeMillis();
    ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));

    Put result = put.addColumn(family, qualifier, timestamp, value);

    assertEquals(1, result.familyMap.size());
    assertTrue(result.has(family, Bytes.toBytes("qualifier"), timestamp, Bytes.toBytes("value")));
  }

  @Test
  public void testAddImmutableWithTag() {
    Put put = new Put(Bytes.toBytes("immutable-row"), true);
    byte[] family = Bytes.toBytes("immutable-family");
    byte[] qualifier = Bytes.toBytes("immutable-qualifier");
    byte[] value = Bytes.toBytes("immutable-value");
    Tag[] tags = new Tag[] { new ArrayBackedTag((byte) 1, "immutable-tag-1") };

    Put result = put.addImmutable(family, qualifier, value, tags);

    assertEquals(1, result.familyMap.size());
    assertTrue(result.has(family, qualifier, value));

    Cell cell = result.get(family, qualifier).get(0);

    assertEquals(family, cell.getFamilyArray());
    assertEquals(qualifier, cell.getQualifierArray());
    assertEquals(value, cell.getValueArray());
    assertEquals(put.getTimeStamp(), cell.getTimestamp());

    List<Tag> cellTags = CellUtil.getTags(cell);

    assertEquals(1, cellTags.size());
    assertEquals((byte) 1, cellTags.get(0).getType());
  }

  @Test
  public void testAddImmutableWithTimestampAndTag() {
    Put put = new Put(Bytes.toBytes("row"), true);
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    long timestamp = System.currentTimeMillis();
    byte[] value = Bytes.toBytes("value");
    Tag[] tags = new Tag[] { new ArrayBackedTag((byte) 1, "tag1") };

    Put result = put.addImmutable(family, qualifier, timestamp, value, tags);

    assertEquals(1, result.familyMap.size());
    assertTrue(result.has(family, qualifier, value));

    Cell cell = result.get(family, qualifier).get(0);

    assertEquals(family, cell.getFamilyArray());
    assertEquals(qualifier, cell.getQualifierArray());
    assertEquals(timestamp, cell.getTimestamp());
    assertEquals(value, cell.getValueArray());

    List<Tag> cellTags = CellUtil.getTags(cell);

    assertEquals(1, cellTags.size());
    assertEquals((byte) 1, cellTags.get(0).getType());
  }

  @Test
  public void testAddImmutableWithByteBufferTimestampAndTag() {
    Put put = new Put(Bytes.toBytes("row"), true);
    byte[] family = Bytes.toBytes("family");
    ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes("qualifier"));
    long timestamp = System.currentTimeMillis();
    ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));
    Tag[] tags = new Tag[] { new ArrayBackedTag((byte) 1, "tag1") };

    Put result = put.addImmutable(family, qualifier, timestamp, value, tags);

    assertEquals(1, result.familyMap.size());
    assertTrue(result.has(family, Bytes.toBytes("qualifier"), timestamp, Bytes.toBytes("value")));

    Cell cell = result.get(family, Bytes.toBytes("qualifier")).get(0);

    assertEquals(family, cell.getFamilyArray());
    assertEquals(Bytes.toBytes(qualifier), cell.getQualifierArray());
    assertEquals(timestamp, cell.getTimestamp());
    assertEquals(Bytes.toBytes(value), cell.getValueArray());

    List<Tag> cellTags = CellUtil.getTags(cell);

    assertEquals(1, cellTags.size());
    assertEquals((byte) 1, cellTags.get(0).getType());
  }

  @Test
  public void testAddImmutableWithByteBufferAndTimestamp() {
    Put put = new Put(Bytes.toBytes("row"), true);
    byte[] family = Bytes.toBytes("family");
    ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes("qualifier"));
    long timestamp = System.currentTimeMillis();
    ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));

    Put result = put.addImmutable(family, qualifier, timestamp, value);

    assertEquals(1, result.familyMap.size());
    assertTrue(result.has(family, Bytes.toBytes("qualifier"), timestamp, Bytes.toBytes("value")));

    Cell cell = result.get(family, Bytes.toBytes("qualifier")).get(0);

    assertEquals(family, cell.getFamilyArray());
    assertEquals(Bytes.toBytes(qualifier), cell.getQualifierArray());
    assertEquals(timestamp, cell.getTimestamp());
    assertEquals(Bytes.toBytes(value), cell.getValueArray());
  }
}
