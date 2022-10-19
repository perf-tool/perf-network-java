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
import com.perftool.network.constant.PerfnConst;
import com.perftool.network.module.TcpMessage;
import com.perftool.network.trace.TraceBean;
import com.perftool.network.trace.TraceReporter;
import com.perftool.network.trace.module.SpanInfo;
import com.perftool.network.util.RandomUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.UUID;

@Slf4j
public class TcpClientThread extends Thread {

    private final ClientConfig clientConfig;

    private final Channel channel;

    private final RateLimiter rateLimiter;

    private final TraceReporter traceReporter;

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
                TcpMessage message = new TcpMessage();
                TraceBean traceBean = new TraceBean();
                ObjectMapper objectMapper = new ObjectMapper();
                SpanInfo spanInfo = new SpanInfo();
                traceBean.setTraceId(UUID.randomUUID().toString());
                long createTime = System.currentTimeMillis();
                traceBean.setCreateTime(createTime);
                spanInfo.setReceiveTime(createTime);
                traceBean.setSpanId(spanInfo);
                message.setTcpHeader(traceBean);
                message.setTcpContent(RandomUtil.randomStr(clientConfig.getPacketSize()));
                byte[] bytes = objectMapper.writeValueAsBytes(message);
                ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
                this.channel.writeAndFlush(byteBuf).addListener(future -> {
                    if (Optional.ofNullable(traceReporter).isPresent()) {
                        traceReporter.reportTrace(traceBean, PerfnConst.COMM_TYPE_CLIENT);
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
