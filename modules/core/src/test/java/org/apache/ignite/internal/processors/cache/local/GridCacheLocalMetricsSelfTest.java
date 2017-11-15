/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheTransactionalAbstractMetricsSelfTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Local cache metrics test.
 */
public class GridCacheLocalMetricsSelfTest extends GridCacheTransactionalAbstractMetricsSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        MemoryConfiguration memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration plc = new MemoryPolicyConfiguration();

        plc.setName("dfltPlc");
        plc.setMaxSize(MemoryConfiguration.DFLT_MEMORY_POLICY_MAX_SIZE * 10);

        memCfg.setDefaultMemoryPolicyName("dfltPlc");
        memCfg.setMemoryPolicies(plc);

        c.setMemoryConfiguration(memCfg);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }
}
