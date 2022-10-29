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

package com.perftool.network.trace.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.perftool.network.trace.TraceReporter;
import com.perftool.network.util.ConfigUtil;
import io.github.perftool.trace.module.TraceBean;
import org.bson.Document;

import java.util.ArrayList;

public class MongoClientImpl implements TraceReporter {

    private MongoConfig config;

    public MongoClientImpl() {
        this.config = ConfigUtil.getMongoConfig();
    }

    public MongoDatabase createMongoDatabase() {
        if ("".equalsIgnoreCase(config.mongodbPassword)
                || "".equalsIgnoreCase(config.mongodbUsername)) {
            MongoClient mongoClient = new MongoClient(config.mongodbHost, config.mongodbPort);
            return mongoClient.getDatabase(config.mongodbDatabaseName);
        } else {
            ServerAddress addr = new ServerAddress(config.mongodbHost, config.mongodbPort);
            ArrayList<ServerAddress> addresses = new ArrayList<>();
            addresses.add(addr);
            MongoCredential credential = MongoCredential.createScramSha1Credential(config.mongodbUsername,
                    config.mongodbDatabaseName,
                    config.mongodbPassword.toCharArray());
            MongoClientOptions options = MongoClientOptions.builder().build();
            MongoClient mongoClient = new MongoClient(addresses, credential, options);
            return mongoClient.getDatabase(config.mongodbDatabaseName);
        }
    }

    @Override
    public void reportTrace(TraceBean traceBean) {
        MongoDatabase database = createMongoDatabase();
        initCollection(database);
        MongoCollection<Document> collection = database.getCollection(config.mongodbCollectionName);
        Document document = new Document("traceId", traceBean.getTraceId())
                .append("spanInfo", traceBean.getSpanInfo());
        collection.insertOne(document);
    }

    private void initCollection(MongoDatabase database) {
        database.createCollection(config.mongodbCollectionName);
    }

}
