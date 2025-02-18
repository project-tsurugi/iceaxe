/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.metadata.TgTableMetadata;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * Iceaxe example (for Java11)
 */
public class Example00 {

    private static final String TABLE_NAME = "TEST";

    public static void main(String... args) throws IOException, InterruptedException {
        // @see Example01Connector
        var endpoint = URI.create("tcp://localhost:12345");
        var credential = new UsernamePasswordCredential("user", "password");
        var connector = TsurugiConnector.of(endpoint, credential);

        // @see Example02Session
        try (var session = connector.createSession()) {
            executeCreateTable(session);
            executeInsert(session);
            executeUpdate(session);
            executeSelect(session);
        }
    }

    /**
     * @see Example11Ddl
     */
    private static void executeCreateTable(TsurugiSession session) throws IOException, InterruptedException {
        var setting = TgTmSetting.of(TgTxOption.ofDDL());
        var tm = session.createTransactionManager(setting);

        Optional<TgTableMetadata> metadata = session.findTableMetadata(TABLE_NAME);
        if (metadata.isPresent()) {
            var sql = "drop table " + TABLE_NAME;
            tm.executeDdl(sql);
        }

        var sql = "create table " + TABLE_NAME + " (\n" //
                + "  FOO int primary key,\n" //
                + "  BAR bigint,\n" //
                + "  ZZZ varchar(10)\n" //
                + ")";
        tm.executeDdl(sql);
    }

    /**
     * @see Example21Insert
     */
    private static void executeInsert(TsurugiSession session) throws IOException, InterruptedException {
        // 更新系のSQLをLTXで実行する場合は、更新対象のテーブル名をwritePreserveに指定する必要がある。
        var setting = TgTmSetting.ofAlways(TgTxOption.ofLTX(TABLE_NAME));
        var tm = session.createTransactionManager(setting);

        var sql = "insert into " + TABLE_NAME + "(FOO, BAR, ZZZ) values(:foo, :bar, :zzz)";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo) //
                .addLong("bar", TestEntity::getBar) //
                .addString("zzz", TestEntity::getZzz);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (int i = 0; i < 10; i++) {
                    var entity = new TestEntity(i, Long.valueOf(i), "z" + i);
                    transaction.executeAndGetCount(ps, entity);
                }
                return;
            });
        }
    }

    /**
     * @see Example41Update
     */
    private static void executeUpdate(TsurugiSession session) throws IOException, InterruptedException {
        // 更新系のSQLをLTXで実行する場合は、更新対象のテーブル名をwritePreserveに指定する必要がある。
        var setting = TgTmSetting.ofAlways(TgTxOption.ofLTX(TABLE_NAME));
        var tm = session.createTransactionManager(setting);

        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "update " + TABLE_NAME + " set BAR=" + bar + " where FOO=" + foo;
        var parameterMapping = TgParameterMapping.of(foo, bar);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (int i = 0; i < 10; i += 2) {
                    var parameter = TgBindParameters.of(foo.bind(i), bar.bind(i + 1));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    /**
     * @see Example31Select
     */
    private static void executeSelect(TsurugiSession session) throws IOException, InterruptedException {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
        var tm = session.createTransactionManager(setting);

        var sql = "select FOO, BAR, ZZZ from " + TABLE_NAME;
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt("FOO", TestEntity::setFoo) //
                .addLong("BAR", TestEntity::setBar) //
                .addString("ZZZ", TestEntity::setZzz);
        try (var ps = session.createQuery(sql, resultMapping)) {
            tm.execute(transaction -> {
                List<TestEntity> list = transaction.executeAndGetList(ps);
                for (var entity : list) {
                    System.out.println(entity);
                }
            });
        }
    }
}
