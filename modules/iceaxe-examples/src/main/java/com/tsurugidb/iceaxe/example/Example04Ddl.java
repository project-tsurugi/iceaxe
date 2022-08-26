package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;

/**
 * ddl example
 */
public class Example04Ddl {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofLTX("TEST"));
            var tm = session.createTransactionManager(setting);

            createTable(session, tm);
            dropTable(session, tm);

            existsTable(session);
            getTableMetadata(session);
        }
    }

    void createTable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "create table TEST" //
                + "(" //
                + "  foo int," // INT4
                + "  bar bigint," // INT8
                + "  zzz varchar(10)," // CHARACTER
                + "  primary key(foo)" //
                + ")";
        try (var ps = session.createPreparedStatement(sql)) {
            // DDLの実行もトランザクション内で行う
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                }
            });
        }
    }

    void dropTable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "drop table TEST";
        try (var ps = session.createPreparedStatement(sql)) {
            // DDLの実行もトランザクション内で行う
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                }
            });
        }
    }

    void existsTable(TsurugiSession session) throws IOException {
        var opt = session.findTableMetadata("TEST");
        if (opt.isPresent()) {
            System.out.println("table exists");
        } else {
            System.out.println("table not exists");
        }
    }

    void getTableMetadata(TsurugiSession session) throws IOException {
        var opt = session.findTableMetadata("TEST");
        if (opt.isPresent()) {
            var metadata = opt.get();
            System.out.println(metadata.getDatabaseName());
            System.out.println(metadata.getSchemaName());
            System.out.println(metadata.getTableName());
            System.out.println(metadata.getLowColumnList());
        }
    }
}
