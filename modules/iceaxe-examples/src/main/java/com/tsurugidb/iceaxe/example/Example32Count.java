package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * select count example
 */
public class Example32Count {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
            var tm = session.createTransactionManager(setting);

            countAll0_execRs(session, tm);
            countAll0_execPs(session, tm);
            countAll0_execTm(session, tm);

            countAllAsInteger(session, tm);

            countGroup(session, tm);
        }
    }

    void countAll0_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select count(*) count from TEST")) {
            int count = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    Optional<TsurugiResultEntity> recordOpt = result.findRecord();
                    return recordOpt.get().getInt4("count");
                }
            });
            System.out.println(count);
        }
    }

    void countAll0_execPs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select count(*) count from TEST")) {
            int count = tm.execute(transaction -> {
                Optional<TsurugiResultEntity> recordOpt = ps.executeAndFindRecord(transaction);
                return recordOpt.get().getInt4("count");
            });
            System.out.println(count);
        }
    }

    void countAll0_execTm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select count(*) count from TEST")) {
            Optional<TsurugiResultEntity> recordOpt = ps.executeAndFindRecord(tm);
            int count = recordOpt.get().getInt4("count");
            System.out.println(count);
        }
    }

    void countAllAsInteger(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var resultMapping = TgResultMapping.of(record -> record.nextInt4());
        try (var ps = session.createPreparedQuery("select count(*) from TEST", resultMapping)) {
            int count = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    Optional<Integer> countOpt = result.findRecord();
                    return countOpt.get();
                }
            });
            System.out.println(count);
        }
    }

    void countGroup(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select ZZZ, count(*) from TEST group by ZZZ", TgResultMapping.of(CountByZzzEntity::of))) {
            List<CountByZzzEntity> list = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    static class CountByZzzEntity {
        private String zzz;
        private int count;

        public String getZzz() {
            return zzz;
        }

        public void setZzz(String zzz) {
            this.zzz = zzz;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public static CountByZzzEntity of(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
            var entity = new CountByZzzEntity();
            entity.setZzz(record.nextCharacter());
            entity.setCount(record.nextInt4());
            return entity;
        }
    }
}
