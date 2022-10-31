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

import com.google.common.util.concurrent.RateLimiter;
import com.perftool.network.config.ClientConfig;
import com.perftool.network.module.TcpMessage;
import com.perftool.network.util.RandomUtil;
import io.github.perftool.trace.module.SpanInfo;
import io.github.perftool.trace.module.TraceBean;
import io.github.perftool.trace.report.ITraceReporter;
import io.github.perftool.trace.util.InboundCounter;
import io.github.perftool.trace.util.Ipv4Util;
import io.github.perftool.trace.util.JacksonUtil;
import io.github.perftool.trace.util.StringTool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class TcpClientThread extends Thread {

    private static final InboundCounter inboundCounter = new InboundCounter(999);

    private static final String formattedIp = StringTool.formatIp(Ipv4Util.getIp("eth0"));

    private final ClientConfig clientConfig;

    private final Channel channel;

    private final RateLimiter rateLimiter;

    private final ITraceReporter traceReporter;

    public TcpClientThread(ClientConfig clientConfig, Channel channel, ITraceReporter traceReporter) {
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
                String tranceId = String.format("%s-%s-%s",
                        createTime,
                        formattedIp,
                        inboundCounter.get());
                TraceBean traceBean = new TraceBean();
                traceBean.setTraceId(tranceId);
                SpanInfo spanInfo = new SpanInfo();
                spanInfo.setSpanId(tranceId);
                traceBean.setSpanInfo(spanInfo);

                TcpMessage message = new TcpMessage();
                message.setTcpHeader(traceBean);
                message.setTcpContent(RandomUtil.randomStr(clientConfig.getPacketSize()));
                byte[] bytes = JacksonUtil.toBytes(message);
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
