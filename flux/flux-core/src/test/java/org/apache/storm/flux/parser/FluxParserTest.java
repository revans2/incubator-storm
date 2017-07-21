/*
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
package org.apache.storm.flux.parser;


import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;


import static org.junit.Assert.assertTrue;

public class FluxParserTest {

    @Test
    public void testCofigParamListParser() throws Exception {
        try (InputStream in = FluxParserTest.class.getResourceAsStream("")) {
            TopologyDef topologyDef = FluxParser.parseResource("/configs/nimbus_seeds.yaml", true, true,
                "src/test/resources/configs/test.properties", false);
            assertTrue(Utils.isValidConf(topologyDef.getConfig()));
            ConfigValidation.validateFields(topologyDef.getConfig());
        }
    }
}