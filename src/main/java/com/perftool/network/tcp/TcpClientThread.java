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

package com.perftool.network.tcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import com.perftool.network.config.ClientConfig;
import com.perftool.network.module.TcpMessage;
import com.perftool.network.trace.TraceReporter;
import com.perftool.network.util.RandomUtil;
import com.perftool.network.util.TransformUtil;
import io.github.perftool.trace.module.SpanInfo;
import io.github.perftool.trace.module.TraceBean;
import io.github.perftool.trace.util.Ipv4Util;
import io.github.perftool.trace.util.StringTool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class TcpClientThread extends Thread {

    private final ClientConfig clientConfig;

    private final Channel channel;

    private final RateLimiter rateLimiter;

    private final TraceReporter traceReporter;

    private final String addr = Ipv4Util.getIp("eth0");

    public TcpClientThread(ClientConfig clientConfig, Channel channel, TraceReporter traceReporter) {
        this.clientConfig = clientConfig;
        this.channel = channel;
        this.traceReporter = traceReporter;
        this.rateLimiter = RateLimiter.create(clientConfig.getTickPerConnMs() / 1000);
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            rateLimiter.acquire();
            try {
                long createTime = System.currentTimeMillis();
                StringBuilder builder = new StringBuilder();
                String[] ips = addr.split("\\.");
                for (String ip : ips) {
                    builder.append(StringTool.fixedLen(ip, 3));
                }
                String tranceId = String.format("%s-%s-%s",
                        createTime,
                        builder,
                        TransformUtil.getIncreaseNumber(999));
                TraceBean traceBean = new TraceBean();
                traceBean.setTraceId(tranceId);
                SpanInfo spanInfo = new SpanInfo();
                spanInfo.setSpanId(tranceId);
                traceBean.setSpanInfo(spanInfo);

                TcpMessage message = new TcpMessage();
                message.setTcpHeader(traceBean);
                message.setTcpContent(RandomUtil.randomStr(clientConfig.getPacketSize()));
                ObjectMapper objectMapper = new ObjectMapper();
                byte[] bytes = objectMapper.writeValueAsBytes(message);
                ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
                this.channel.writeAndFlush(byteBuf).addListener(future -> {
                    if (Optional.ofNullable(traceReporter).isPresent()) {
                        traceReporter.reportTrace(traceBean);
                    }
                    if (!future.isSuccess()) {
                        log.error("send msg error", future.cause());
                    } else {
                        log.info("send msg success.");
                    }
                });
            } catch (Exception e) {
                log.error("send tcp request fail ", e);
                break;
            }
        }
    }

}
