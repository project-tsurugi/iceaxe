package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * select count example
 */
public class Example32Count {

    void main() throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
            var tm = session.createTransactionManager(setting);

            countAll_executeQuery(session, tm);
            countAll_executeAndFindRecord(session, tm);
            countAll_tm(session, tm);
            countAll_tm_sql(tm);

            countAllAsInteger(session, tm);
            countAllAsInteger_singleColumn(session, tm);

            countGroup(session, tm);
        }
    }

    void countAll_executeQuery(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select count(*) count from TEST")) {
            int count = tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    Optional<TsurugiResultEntity> recordOpt = result.findRecord();
                    return recordOpt.get().getInt("count");
                }
            });
            System.out.println(count);
        }
    }

    void countAll_executeAndFindRecord(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select count(*) count from TEST")) {
            int count = tm.execute(transaction -> {
                Optional<TsurugiResultEntity> recordOpt = transaction.executeAndFindRecord(ps);
                return recordOpt.get().getInt("count");
            });
            System.out.println(count);
        }
    }

    void countAll_tm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select count(*) count from TEST")) {
            Optional<TsurugiResultEntity> recordOpt = tm.executeAndFindRecord(ps);
            int count = recordOpt.get().getInt("count");
            System.out.println(count);
        }
    }

    void countAll_tm_sql(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        Optional<TsurugiResultEntity> recordOpt = tm.executeAndFindRecord("select count(*) count from TEST");
        int count = recordOpt.get().getInt("count");
        System.out.println(count);
    }

    void countAllAsInteger(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var resultMapping = TgResultMapping.of(record -> record.nextInt());
        try (var ps = session.createQuery("select count(*) from TEST", resultMapping)) {
            int count = tm.execute(transaction -> {
                Optional<Integer> countOpt = transaction.executeAndFindRecord(ps);
                return countOpt.get();
            });
            System.out.println(count);
        }
    }

    void countAllAsInteger_singleColumn(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var resultMapping = TgResultMapping.ofSingle(int.class);
        try (var ps = session.createQuery("select count(*) from TEST", resultMapping)) {
            int count = tm.execute(transaction -> {
                Optional<Integer> countOpt = transaction.executeAndFindRecord(ps);
                return countOpt.get();
            });
            System.out.println(count);
        }
    }

    void countGroup(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select ZZZ, count(*) from TEST group by ZZZ";
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                return transaction.executeAndGetList(ps);
            });
            for (TsurugiResultEntity entity : list) {
                System.out.printf("%s: %d%n", entity.getString("ZZZ"), entity.getInt(entity.getName(1)));
            }
        }
    }

    void countGroup_userEntity(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select ZZZ, count(*) from TEST group by ZZZ";
        var resultMapping = TgResultMapping.of(CountByZzzEntity::of);
        try (var ps = session.createQuery(sql, resultMapping)) {
            List<CountByZzzEntity> list = tm.execute(transaction -> {
                return transaction.executeAndGetList(ps);
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

        public static CountByZzzEntity of(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
            var entity = new CountByZzzEntity();
            entity.setZzz(record.nextString());
            entity.setCount(record.nextInt());
            return entity;
        }
    }
}
