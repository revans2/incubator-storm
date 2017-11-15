/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HBaseStoreResult implements Iterable<Cell[]> {

    private int numFamilies;
    private int numVersions;
    private Cell[] cellList;

    public HBaseStoreResult(Result result) {
        this.cellList = result.rawCells();
        this.numVersions = 0;

        long previousTS = Long.MAX_VALUE;
        long currentTS  = 0;
        for (Cell cell : cellList) {
            currentTS = cell.getTimestamp();
            if (currentTS >= previousTS)
                break;
            previousTS = currentTS;
            ++numVersions;
        }

        this.numFamilies = cellList.length / numVersions;
    }

    public int getNumVersions() {
        return this.numVersions;
    }

    public Iterator<Cell[]> iterator() {
        return new HBaseStoreResultIterator();
    }

    class HBaseStoreResultIterator implements Iterator<Cell[]> {
        int current = 0;

        public boolean hasNext() {
            return current < numVersions;
        }

        public Cell[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Cell[] versionCells = new Cell[numFamilies];
            for (int i = 0; i < numFamilies; ++i) {
                versionCells[i] = cellList[current + (i * numVersions)];
            }

            ++current;
            return versionCells;
        }
    }

}
