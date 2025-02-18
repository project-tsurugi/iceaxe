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

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * ddl example
 */
public class Example11Ddl {

    public static void main(String... args) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            new Example11Ddl().main(session);
        }
    }

    void main(TsurugiSession session) throws IOException, InterruptedException {
        // TsurugiTransactionManagerのexecuteDdlメソッドを使う場合、
        // TgTmSettingが指定されていなかったら、暗黙にLTXとして実行する。
        var tm = session.createTransactionManager();

        if (existsTable(session)) {
            dropTable(tm);
        }
        createTable(tm);

        getTableMetadata(session);
    }

    void createTable(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "create table TEST" //
                + "(" //
                + "  FOO int," // INT4
                + "  BAR bigint," // INT8
                + "  ZZZ varchar(10)," // CHARACTER
                + "  primary key(FOO)" //
                + ")";
        tm.executeDdl(sql);
    }

    void dropTable(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "drop table TEST";
        tm.executeDdl(sql);
    }

    boolean existsTable(TsurugiSession session) throws IOException, InterruptedException {
        var metadataOpt = session.findTableMetadata("TEST");
        if (metadataOpt.isPresent()) {
            System.out.println("table exists");
            return true;
        } else {
            System.out.println("table not exists");
            return false;
        }
    }

    void getTableMetadata(TsurugiSession session) throws IOException, InterruptedException {
        var metadataOpt = session.findTableMetadata("TEST");
        if (metadataOpt.isPresent()) {
            var metadata = metadataOpt.get();
            System.out.println(metadata.getDatabaseName());
            System.out.println(metadata.getSchemaName());
            System.out.println(metadata.getTableName());
            System.out.println(metadata.getLowColumnList());
        }
    }

    public static void dropAndCreateTable(TsurugiSession session) throws IOException, InterruptedException {
        var tm = session.createTransactionManager(TgTxOption.ofDDL());
        tm.execute(transaction -> {
            if (transaction.getSession().findTableMetadata("TEST").isPresent()) {
                transaction.executeDdl("drop table TEST");
            }
            transaction.executeDdl("create table TEST" //
                    + "(" //
                    + "  FOO int," // INT4
                    + "  BAR bigint," // INT8
                    + "  ZZZ varchar(10)," // CHARACTER
                    + "  primary key(FOO)" //
                    + ")");
        });
    }
}
