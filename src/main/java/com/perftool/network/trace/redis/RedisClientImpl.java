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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.perftool.network.trace.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.perftool.network.trace.TraceBean;
import com.perftool.network.trace.TraceReporter;
import com.perftool.network.trace.redis.functional.SyncCommandCallback;
import com.perftool.network.util.ConfigUtil;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;

@Slf4j
public class RedisClientImpl implements TraceReporter {

    private RedisConfig redisConfig;

    private GenericObjectPool<StatefulRedisConnection<String, String>> redisConnectionPool;

    public RedisClientImpl() {
        this.redisConfig = ConfigUtil.getRedisConfig();
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(this.redisConfig.maxActive);
        poolConfig.setMaxIdle(this.redisConfig.maxIdle);
        poolConfig.setMinIdle(this.redisConfig.minIdle);
        redisConnectionPool = ConnectionPoolSupport.createGenericObjectPool(() -> redisClient().connect(), poolConfig);
    }

    private RedisClient redisClient() {
        String[] url = redisConfig.clusterNodeUrl.split(":");
        RedisURI redisUrl = RedisURI.Builder
                .redis(url[0], Integer.parseInt(url[1]))
                .withDatabase(redisConfig.database)
                .withTimeout(Duration.ofSeconds(redisConfig.timeout))
                .withAuthentication(redisConfig.user, redisConfig.password.toCharArray())
                .build();
        return RedisClient.create(redisUrl);
    }

    private <T> T executeSync(SyncCommandCallback<T> callback) {
        try (StatefulRedisConnection<String, String> connection = redisConnectionPool.borrowObject()) {
            connection.setAutoFlushCommands(true);
            RedisCommands<String, String> commands = connection.sync();
            return callback.doInConnection(commands);
        } catch (Exception e) {
            log.warn("executeSync redis failed.", e);
            throw new RuntimeException(e);
        }
    }

    public String set(String key, String value) {
        return executeSync(commands -> commands.set(key, value));
    }

    public String get(String key) {
        return executeSync(commands -> commands.get(key));
    }

    public Long del(String... key) {
        return executeSync(commands -> commands.del(key));
    }

    public KeyScanCursor<String> scan(ScanArgs scanArgs) {
        return executeSync(commands -> commands.scan(scanArgs));
    }

    @Override
    public void reportTrace(TraceBean traceBean, String commType) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            set(commType + "-traceid:" + traceBean.getTraceId(), mapper.writeValueAsString(traceBean));
        } catch (Exception e) {
         log.error("to json fail. traceId: {}, createTime: {}",
                 traceBean.getTraceId(), traceBean.getCreateTime());
        }

    }
}
