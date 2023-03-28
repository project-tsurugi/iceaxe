package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * ddl example
 */
public class Example11Ddl {

    void main() throws IOException {
        try (var session = Example02Session.createSession()) {
            // TsurugiTransactionManagerのexecuteDdlメソッドを使う場合、
            // TgTmSettingが指定されていなかったら、暗黙にLTXとして実行する。
            var tm = session.createTransactionManager();
            createTable(tm);
            dropTable(tm);

            existsTable(session);
            getTableMetadata(session);

            dropAndCreateTable(session);
        }
    }

    void createTable(TsurugiTransactionManager tm) throws IOException {
        var sql = "create table TEST" //
                + "(" //
                + "  FOO int," // INT4
                + "  BAR bigint," // INT8
                + "  ZZZ varchar(10)," // CHARACTER
                + "  primary key(FOO)" //
                + ")";
        tm.executeDdl(sql);
    }

    void dropTable(TsurugiTransactionManager tm) throws IOException {
        var sql = "drop table TEST";
        tm.executeDdl(sql);
    }

    void existsTable(TsurugiSession session) throws IOException {
        var metadataOpt = session.findTableMetadata("TEST");
        if (metadataOpt.isPresent()) {
            System.out.println("table exists");
        } else {
            System.out.println("table not exists");
        }
    }

    void getTableMetadata(TsurugiSession session) throws IOException {
        var metadataOpt = session.findTableMetadata("TEST");
        if (metadataOpt.isPresent()) {
            var metadata = metadataOpt.get();
            System.out.println(metadata.getDatabaseName());
            System.out.println(metadata.getSchemaName());
            System.out.println(metadata.getTableName());
            System.out.println(metadata.getLowColumnList());
        }
    }

    void dropAndCreateTable(TsurugiSession session) throws IOException {
        // DDLをLTXで実行する場合は、writePreserveには何も指定しなくてよい。
        var tm = session.createTransactionManager(TgTxOption.ofLTX());
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
