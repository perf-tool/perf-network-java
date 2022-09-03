/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.perftool.network.util;

import com.perftool.network.config.ClientConfig;
import com.perftool.network.config.ServerConfig;

public class ConfigUtil {

    public static ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setHost(EnvUtil.getString("CLIENT_HOST", "localhost"));
        clientConfig.setPort(EnvUtil.getInt("CLIENT_PORT", 5678));
        clientConfig.setConnNum(EnvUtil.getInt("CLIENT_CONN_NUM", 10));
        clientConfig.setTickPerConnMs(EnvUtil.getInt("CLIENT_TICK_PER_CONN_MS", 1000));
        clientConfig.setPacketSize(EnvUtil.getInt("CLIENT_PACKET_SIZE", 1024));
        return clientConfig;
    }

    public static ServerConfig getServerConfig() {
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setHost(EnvUtil.getString("SERVER_HOST", "0.0.0.0"));
        serverConfig.setPort(EnvUtil.getInt("SERVER_PORT", 5678));
        return serverConfig;
    }

}
