/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.scheduler.resource;

/**
 * A class to hold global variables
 */
public final class RAS_TYPES {

    public static final String TYPE_MEMORY = "memory";
    public static final Double DEFAULT_ONHEAP_MEMORY_REQUIREMENT = 100.0;
    public static final Double DEFAULT_OFFHEAP_MEMORY_REQUIREMENT = 50.0;
    public static final String TYPE_MEMORY_ONHEAP = "onHeap";
    public static final String TYPE_MEMORY_OFFHEAP = "offHeap";

    public static final String TYPE_CPU = "cpu";
    public static final String TYPE_CPU_TOTAL = "total";
    public static final Double DEFAULT_CPU_REQUIREMENT = 10.0;

}
