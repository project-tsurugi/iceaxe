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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * example to convert data type
 */
public class Example94TypeConvert {

    /**
     * example to convert data type
     */
    private static final IceaxeConvertUtil CONVERT_UTIL = new IceaxeConvertUtil() {
        private final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd");

        @Override
        protected String convertString(Object obj) {
            if (obj instanceof LocalDate) {
                return ((LocalDate) obj).format(DATE_FORMATTER);
            }
            return super.convertString(obj);
        }

        @Override
        protected LocalDate convertLocalDate(Object obj) {
            if (obj instanceof String) {
                return LocalDate.parse((String) obj, DATE_FORMATTER);
            }
            return super.convertLocalDate(obj);
        }
    };

    public static void main(String... args) throws IOException, InterruptedException {
        new Example94TypeConvert().main();
    }

    void main() throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            createTable(tm);
        }

        var entity = new Example94Entity();
        entity.setKey(1);
        entity.setDate(LocalDate.of(2022, 7, 13));
        insert1(entity);
        entity.setKey(2);
        insert2(entity);

        select1();
        select2();
        select3();
    }

    void createTable(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "create table example94 (" //
                + "key1 int," //
                + "date1 char(10)," // yyyy/MM/dd
                + "primary key(key1)" //
                + ")";
        tm.executeDdl(sql);
    }

    static class Example94Entity {
        private int key;
        private LocalDate date;

        public void setKey(int key) {
            this.key = key;
        }

        public int getKey() {
            return this.key;
        }

        public void setDate(LocalDate date) {
            this.date = date;
        }

        public LocalDate getDate() {
            return this.date;
        }

        @Override
        public String toString() {
            return "Example94Entity{key=" + key + ", date=" + date + "}";
        }
    }

    void insert1(Example94Entity entity) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            session.setConvertUtil(CONVERT_UTIL); // set session

            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "insert into example94 (key1, date1) values(:key, :date)";
            var parameterMapping = TgParameterMapping.of(Example94Entity.class) //
                    .addInt("key", Example94Entity::getKey) //
                    .add("date", TgDataType.STRING, Example94Entity::getDate);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                tm.executeAndGetCount(ps, entity);
            } catch (IOException e) {
                System.out.println("insert1----");
                e.printStackTrace();
            }
        }
    }

    void insert2(Example94Entity entity) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "insert into example94 (key1, date1) values(:key, :date)";
            var parameterMapping = TgParameterMapping.of(Example94Entity.class) //
                    .addInt("key", Example94Entity::getKey) //
                    .add("date", TgDataType.STRING, Example94Entity::getDate) //
                    .setConvertUtil(CONVERT_UTIL); // set parameter mapping (for 'add' method)
            try (var ps = session.createStatement(sql, parameterMapping)) {
                tm.executeAndGetCount(ps, entity);
            } catch (IOException e) {
                System.out.println("insert2----");
                e.printStackTrace();
            }
        }
    }

    void select1() throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            session.setConvertUtil(CONVERT_UTIL); // set session

            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "select * from example94";
            var resultMapping = TgResultMapping.of(Example94Entity::new) //
                    .addInt(Example94Entity::setKey) //
                    .addDate(Example94Entity::setDate);
            try (var ps = session.createQuery(sql, resultMapping)) {
                var list = tm.executeAndGetList(ps);
                System.out.println(list);
            }
        }
    }

    void select2() throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "select * from example94";
            var resultMapping = TgResultMapping.of(Example94Entity::new) //
                    .addInt(Example94Entity::setKey) //
                    .addDate(Example94Entity::setDate) //
                    .setConvertUtil(CONVERT_UTIL); // set result mapping
            try (var ps = session.createQuery(sql, resultMapping)) {
                var list = tm.executeAndGetList(ps);
                System.out.println(list);
            }
        }
    }

    void select3() throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "select * from example94";
            try (var ps = session.createQuery(sql)) {
                tm.execute(transaction -> {
                    try (var result = transaction.executeQuery(ps)) {
                        for (TsurugiResultEntity entity : result) {
                            entity.setConvertUtil(CONVERT_UTIL); // set result entity

                            int key = entity.getInt("key1");
                            String s = entity.getString("date1");
                            LocalDate d = entity.getDate("date1");
                            System.out.printf("key=%d, s=%s, d=%s\n", key, s, d);
                        }
                    }
                });
            }
        }
    }
}
