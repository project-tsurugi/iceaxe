/*
 * Copyright 2023-2024 Project Tsurugi.
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

    public static void main(String... args) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            Example11Ddl.dropAndCreateTable(session);
            Example21Insert.insert(session);

            new Example32Count().main(session);
        }
    }

    void main(TsurugiSession session) throws IOException, InterruptedException {
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
        var tm = session.createTransactionManager(setting);

        countAll_executeQuery(session, tm);
        countAll_executeAndFindRecord(session, tm);
        countAll_tm(session, tm);
        countAll_tm_sql(tm);
        countAll_tm_sql_index(tm);

        countAllAsInteger(session, tm);
        countAllAsInteger_singleColumn(session, tm);

        countGroup(session, tm);
        countGroup_userEntity(session, tm);
    }

    void countAll_executeQuery(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select count(*) cnt from TEST")) {
            int count = tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    Optional<TsurugiResultEntity> recordOpt = result.findRecord();
                    return recordOpt.get().getInt("cnt");
                }
            });
            System.out.println(count);
        }
    }

    void countAll_executeAndFindRecord(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select count(*) cnt from TEST")) {
            int count = tm.execute(transaction -> {
                Optional<TsurugiResultEntity> recordOpt = transaction.executeAndFindRecord(ps);
                return recordOpt.get().getInt("cnt");
            });
            System.out.println(count);
        }
    }

    void countAll_tm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select count(*) cnt from TEST")) {
            Optional<TsurugiResultEntity> recordOpt = tm.executeAndFindRecord(ps);
            int count = recordOpt.get().getInt("cnt");
            System.out.println(count);
        }
    }

    void countAll_tm_sql(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        Optional<TsurugiResultEntity> recordOpt = tm.executeAndFindRecord("select count(*) cnt from TEST");
        int count = recordOpt.get().getInt("cnt");
        System.out.println(count);
    }

    void countAll_tm_sql_index(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        Optional<TsurugiResultEntity> recordOpt = tm.executeAndFindRecord("select count(*) from TEST");
        int count = recordOpt.get().getInt(0);
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
        var sql = "select ZZZ, count(*) from TEST group by ZZZ order by ZZZ";
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                return transaction.executeAndGetList(ps);
            });
            for (TsurugiResultEntity entity : list) {
                System.out.printf("%s: %d%n", entity.getString("ZZZ"), entity.getInt(1));
            }
        }
    }

    void countGroup_userEntity(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select ZZZ, count(*) from TEST group by ZZZ order by ZZZ";
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

        @Override
        public String toString() {
            return "CountByZzzEntity{zzz=" + zzz + ", count=" + count + "}";
        }

        public static CountByZzzEntity of(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
            var entity = new CountByZzzEntity();
            entity.setZzz(record.nextString());
            entity.setCount(record.nextInt());
            return entity;
        }
    }
}
