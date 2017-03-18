/*
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category({ SmallTests.class, ClientTests.class })
public class TestAppend {
    @Test
    public void testAddColumn() {
        Append append = new Append(Bytes.toBytes("row"));
        byte[] family = Bytes.toBytes("family");
        byte[] qualifier = Bytes.toBytes("qualifier");
        byte[] value = Bytes.toBytes("value");

        Append result = append.addColumn(family, qualifier, value);

        assertEquals(1, result.familyMap.size());

        Cell cell = append.familyMap.firstEntry().getValue().get(0);

        assertEquals(family, cell.getFamilyArray());
        assertEquals(qualifier, cell.getQualifierArray());
        assertEquals(value, cell.getValueArray());
        assertEquals(append.getTimeStamp(), cell.getTimestamp());
    }

    @Test
    public void testAddColumnWithTimestamp() {
        Append append = new Append(Bytes.toBytes("row"));
        byte[] family = Bytes.toBytes("family");
        byte[] qualifier = Bytes.toBytes("qualifier");
        byte[] value = Bytes.toBytes("value");
        long timestamp = System.currentTimeMillis();

        Append result = append.addColumn(family, qualifier, timestamp, value);

        assertEquals(1, result.familyMap.size());

        Cell cell = append.familyMap.firstEntry().getValue().get(0);

        assertEquals(family, cell.getFamilyArray());
        assertEquals(qualifier, cell.getQualifierArray());
        assertEquals(timestamp, cell.getTimestamp());
        assertEquals(value, cell.getValueArray());
    }

    @Test
    public void testAddColumnByteBuffer() {
        Append append = new Append(Bytes.toBytes("row"));
        byte[] family = Bytes.toBytes("family");
        ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes("qualifier"));
        long timestamp = System.currentTimeMillis();
        ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));

        Append result = append.addColumn(family, qualifier, timestamp, value);

        assertEquals(1, result.familyMap.size());

        Cell cell = append.familyMap.firstEntry().getValue().get(0);

        assertEquals(family, cell.getFamilyArray());
        assertEquals(Bytes.toBytes(qualifier), cell.getQualifierArray());
        assertEquals(timestamp, cell.getTimestamp());
        assertEquals(Bytes.toBytes(value), cell.getValueArray());
    }

    @Test
    public void testAddImmutableWithTag() {
        Append append = new Append(Bytes.toBytes("row"), true);
        byte[] family = Bytes.toBytes("family");
        byte[] qualifier = Bytes.toBytes("qualifier");
        byte[] value = Bytes.toBytes("value");
        Tag[] tags = new Tag[] { new ArrayBackedTag((byte) 1, "tag1") };

        Append result = append.addImmutable(family, qualifier, value, tags);

        assertEquals(1, result.familyMap.size());

        Cell cell = append.familyMap.firstEntry().getValue().get(0);

        assertEquals(family, cell.getFamilyArray());
        assertEquals(qualifier, cell.getQualifierArray());
        assertEquals(value, cell.getValueArray());
        assertEquals(append.getTimeStamp(), cell.getTimestamp());

        List<Tag> cellTags = CellUtil.getTags(cell);

        assertEquals(1, cellTags.size());
        assertEquals((byte) 1, cellTags.get(0).getType());
    }

    @Test
    public void testAddImmutableWithTimestampAndTag() {
        Append append = new Append(Bytes.toBytes("row"), true);
        byte[] family = Bytes.toBytes("family");
        byte[] qualifier = Bytes.toBytes("qualifier");
        long timestamp = System.currentTimeMillis();
        byte[] value = Bytes.toBytes("value");
        Tag[] tags = new Tag[] { new ArrayBackedTag((byte) 1, "tag1") };

        Append result = append.addImmutable(family, qualifier, timestamp, value, tags);

        assertEquals(1, result.familyMap.size());

        Cell cell = append.familyMap.firstEntry().getValue().get(0);

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
        Append append = new Append(Bytes.toBytes("row"), true);
        byte[] family = Bytes.toBytes("family");
        ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes("qualifier"));
        long timestamp = System.currentTimeMillis();
        ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));
        Tag[] tags = new Tag[] { new ArrayBackedTag((byte) 1, "tag1") };

        Append result = append.addImmutable(family, qualifier, timestamp, value, tags);

        assertEquals(1, result.familyMap.size());

        Cell cell = append.familyMap.firstEntry().getValue().get(0);

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
        Append append = new Append(Bytes.toBytes("row"), true);
        byte[] family = Bytes.toBytes("family");
        ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes("qualifier"));
        long timestamp = System.currentTimeMillis();
        ByteBuffer value = ByteBuffer.wrap(Bytes.toBytes("value"));

        Append result = append.addImmutable(family, qualifier, timestamp, value);

        assertEquals(1, result.familyMap.size());

        Cell cell = append.familyMap.firstEntry().getValue().get(0);

        assertEquals(family, cell.getFamilyArray());
        assertEquals(Bytes.toBytes(qualifier), cell.getQualifierArray());
        assertEquals(timestamp, cell.getTimestamp());
        assertEquals(Bytes.toBytes(value), cell.getValueArray());
    }
}
