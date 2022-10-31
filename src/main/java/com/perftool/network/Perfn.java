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

package com.perftool.network;

import com.perftool.network.constant.PerfnConst;
import com.perftool.network.metrics.MetricsHandler;
import com.perftool.network.tcp.TcpClientService;
import com.perftool.network.tcp.TcpServerService;
import com.perftool.network.util.ConfigUtil;
import com.sun.net.httpserver.HttpServer;
import io.github.perftool.trace.report.ITraceReporter;
import io.github.perftool.trace.report.ReportUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;

@Slf4j
public class Perfn {

    public static void start(String protocolType, String commType, boolean prometheusMetricsDisable) throws Exception {
        if (!prometheusMetricsDisable) {
            new Thread(() -> {
                try {
                    HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", 20008), 0);
                    httpServer.createContext("/metrics", new MetricsHandler());
                    httpServer.start();
                } catch (IOException e) {
                    log.error("start prometheus metrics server error", e);
                }
            }).start();
        }
        ITraceReporter traceReporter = ReportUtil.getReporter();
        if (PerfnConst.PROTOCOL_TYPE_TCP.equals(protocolType)) {
            if (PerfnConst.COMM_TYPE_CLIENT.equals(commType)) {
                TcpClientService.run(ConfigUtil.getClientConfig(), traceReporter);
            } else if (PerfnConst.COMM_TYPE_SERVER.equals(commType)) {
                TcpServerService.run(ConfigUtil.getServerConfig());
            }
        }
    }

}
