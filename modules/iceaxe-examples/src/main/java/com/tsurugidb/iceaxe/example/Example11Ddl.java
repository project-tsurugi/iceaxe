package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * ddl example
 */
public class Example11Ddl {

    void main() throws IOException {
        try (var session = Example02Session.createSession()) {
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
            // ただしcommit/rollbackは無効（commitしなくてもテーブルは作られるし、rollbackしても消えない）
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
            // ただしcommit/rollbackは無効（commitしなくてもテーブルは削除されるし、rollbackしても復活しない）
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
