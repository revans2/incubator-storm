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

import org.apache.storm.generated.Window;

import java.util.Arrays;

public class TimeRange {
    public Long startTime;
    public Long endTime;
    public Window window;

    public TimeRange(Long startTime, Long endTime, Window window) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.window = window;
    }

    public TimeRange() {
        this.startTime = null;
        this.endTime = null;
        this.window = Window.ALL; //?
    }

    public boolean contains(Long time) {
        if ((startTime == null || (startTime != null && time >= startTime)) &&
                (endTime == null || (endTime != null && time <= endTime))) {
            return true;
        }
        return startTime == null && endTime == null;
    }

    public boolean contains(TimeRange other) {
        return (startTime == null || startTime <= other.startTime) &&
                (endTime == null || other.endTime <= endTime) &&
                window == other.window;
    }

    public boolean overlaps(TimeRange other) {
        return contains(other.endTime) || contains(other.startTime);
    }

    public String toString() {
        return "start: " + this.startTime + " end: " + this.endTime + " window: " + this.window;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{
                startTime,
                endTime,
                window.getValue()
        });
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        TimeRange rhs = (TimeRange) o;
        return rhs.startTime == this.startTime &&
                rhs.endTime == this.endTime &&
                rhs.window == this.window;
    }
}
