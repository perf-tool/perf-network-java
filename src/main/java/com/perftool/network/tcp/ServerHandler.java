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

package com.perftool.network.tcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.perftool.network.module.TcpMessage;
import com.perftool.network.trace.TraceReporter;
import com.perftool.network.trace.mongo.MongoClientImpl;
import com.perftool.network.trace.redis.RedisClientImpl;
import com.perftool.network.util.EnvUtil;
import com.perftool.network.util.TransformUtil;
import io.github.perftool.trace.module.SpanInfo;
import io.github.perftool.trace.module.TraceBean;
import io.github.perftool.trace.util.Ipv4Util;
import io.github.perftool.trace.util.StringTool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
@ChannelHandler.Sharable
public class ServerHandler extends ChannelInboundHandlerAdapter {

    private static TraceReporter traceReporter = null;

    private final  String addr = Ipv4Util.getIp("eth0");
    private static String traceType = EnvUtil.getString("TRACE_TYPE", "DUMMY");

    static {
        switch (traceType) {
            case "MONGO" -> traceReporter = new MongoClientImpl();
            case "REDIS" -> traceReporter = new RedisClientImpl();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            try {
                if (in.readableBytes() > 0) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    TcpMessage tcpMessage = objectMapper.readValue(in.toString(CharsetUtil.UTF_8), TcpMessage.class);
                    TraceBean traceBean = tcpMessage.getTcpHeader();
                    if (Optional.ofNullable(traceBean).isPresent()) {
                        StringBuilder builder = new StringBuilder();
                        String[] ips = addr.split("\\.");
                        for (String ip : ips) {
                            builder.append(StringTool.fixedLen(ip, 3));
                        }
                        String spanId = String.format("%s-%s-%s",
                                System.currentTimeMillis(),
                                builder,
                                TransformUtil.getIncreaseNumber(999));
                        SpanInfo spanInfo = new SpanInfo();
                        spanInfo.setSpanId(spanId);
                        traceBean.setSpanInfo(spanInfo);
                        traceReporter.reportTrace(traceBean);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                ReferenceCountUtil.release(in);
            }
        }
        ctx.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

}
