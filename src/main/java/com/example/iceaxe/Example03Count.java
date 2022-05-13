package com.example.iceaxe;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.tsurugi.iceaxe.TsurugiConnector;
import com.tsurugi.iceaxe.result.TgResultMapping;
import com.tsurugi.iceaxe.result.TsurugiResultEntity;
import com.tsurugi.iceaxe.result.TsurugiResultRecord;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TsurugiSession;

/**
 * select count example
 */
public class Example03Count {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("dbname");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            countAll0_execRs(session);
            countAll0_execPs(session);
            countAll0_execTm(session);

            countAllAsInteger(session);

            countGroup(session);
        }
    }

    void countAll0_execRs(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

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

    void countAll0_execPs(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select count(*) count from TEST")) {
            int count = tm.execute(transaction -> {
                Optional<TsurugiResultEntity> recordOpt = ps.executeAndFindRecord(transaction);
                return recordOpt.get().getInt4("count");
            });
            System.out.println(count);
        }
    }

    void countAll0_execTm(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select count(*) count from TEST")) {
            Optional<TsurugiResultEntity> recordOpt = ps.executeAndFindRecord(tm);
            int count = recordOpt.get().getInt4("count");
            System.out.println(count);
        }
    }

    void countAllAsInteger(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

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

    void countGroup(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

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

        public static CountByZzzEntity of(TsurugiResultRecord record) throws IOException {
            var entity = new CountByZzzEntity();
//TODO      entity.setZzz(record.nextCharacter());
            entity.setCount(record.nextInt4());
            return entity;
        }
    }
}
