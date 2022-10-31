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

import com.perftool.network.config.ClientConfig;
import com.perftool.network.util.EventLoopUtil;
import io.github.perftool.trace.report.ITraceReporter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import java.util.ArrayList;
import java.util.List;

public class TcpClientService {
    public static void run(ClientConfig clientConfig, ITraceReporter traceReporter) throws Exception {
        EventLoopGroup group = EventLoopUtil.newEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(EventLoopUtil.getClientSocketChannelClass(group))
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            ChannelPipeline p = socketChannel.pipeline();
                            p.addLast(new ClientHandler());
                        }
                    });
            List<Channel> channelList = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                Channel channel = bootstrap.connect(clientConfig.getHost(), clientConfig.getPort()).sync().channel();
                new TcpClientThread(clientConfig, channel, traceReporter).start();
                channelList.add(channel);
            }
            for (Channel channel : channelList) {
                channel.closeFuture().sync();
            }
        } finally {
            group.shutdownGracefully();
        }
    }
}
