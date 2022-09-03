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
import com.perftool.network.util.RandomUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TcpClientThread extends Thread {

    private final ClientConfig clientConfig;

    private final Channel channel;

    private final RateLimiter rateLimiter;

    public TcpClientThread(ClientConfig clientConfig, Channel channel) {
        this.clientConfig = clientConfig;
        this.channel = channel;
        this.rateLimiter = RateLimiter.create(clientConfig.getTickPerConnMs() / 1000);
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            rateLimiter.acquire();
            try {
                ByteBuf byteBuf = Unpooled.wrappedBuffer(RandomUtil.randomBytes(clientConfig.getPacketSize()));
                this.channel.writeAndFlush(byteBuf).addListener(future -> {
                    if (!future.isSuccess()) {
                        log.error("send msg error", future.cause());
                    }
                });
            } catch (Exception e) {
                log.error("send tcp request fail ", e);
                break;
            }
        }
    }

}
