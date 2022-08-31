package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
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

    public static void main(String... args) throws IOException {
        new Example94TypeConvert().main();
    }

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            createTable(session, tm);
        }

        var entity = new Example94Entity();
        entity.setKey(1);
        entity.setDate(LocalDate.of(2022, 7, 13));
        insert1(connector, entity);
        entity.setKey(2);
        insert2(connector, entity);

        select1(connector);
        select2(connector);
        select3(connector);
    }

    void createTable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "create table example94 (" //
                + "key1 int," //
                + "date1 char(10)," // yyyy/MM/dd
                + "primary key(key1)" //
                + ")";
        try (var ps = session.createPreparedStatement(sql)) {
            ps.executeAndGetCount(tm);
        } catch (IOException e) {
            System.out.println("createTable----");
            e.printStackTrace();
        }
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

    void insert1(TsurugiConnector connector, Example94Entity entity) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
            session.setConvertUtil(CONVERT_UTIL); // set session

            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "insert into example94 (key1, date1) values(:key, :date)";
            var parameterMapping = TgParameterMapping.of(Example94Entity.class) //
                    .int4("key", Example94Entity::getKey) //
                    .add("date", TgDataType.CHARACTER, Example94Entity::getDate);
            try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
                ps.executeAndGetCount(tm, entity);
            } catch (IOException e) {
                System.out.println("insert1----");
                e.printStackTrace();
            }
        }
    }

    void insert2(TsurugiConnector connector, Example94Entity entity) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "insert into example94 (key1, date1) values(:key, :date)";
            var parameterMapping = TgParameterMapping.of(Example94Entity.class) //
                    .int4("key", Example94Entity::getKey) //
                    .add("date", TgDataType.CHARACTER, Example94Entity::getDate) //
                    .convertUtil(CONVERT_UTIL); // set parameter mapping (for 'add' method)
            try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
                ps.executeAndGetCount(tm, entity);
            } catch (IOException e) {
                System.out.println("insert2----");
                e.printStackTrace();
            }
        }
    }

    void select1(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
            session.setConvertUtil(CONVERT_UTIL); // set session

            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "select * from example94";
            var resultMapping = TgResultMapping.of(Example94Entity::new) //
                    .int4(Example94Entity::setKey) //
                    .date(Example94Entity::setDate);
            try (var ps = session.createPreparedQuery(sql, resultMapping)) {
                var list = ps.executeAndGetList(tm);
                System.out.println(list);
            }
        }
    }

    void select2(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "select * from example94";
            var resultMapping = TgResultMapping.of(Example94Entity::new) //
                    .int4(Example94Entity::setKey) //
                    .date(Example94Entity::setDate) //
                    .convertUtil(CONVERT_UTIL); // set result mapping
            try (var ps = session.createPreparedQuery(sql, resultMapping)) {
                var list = ps.executeAndGetList(tm);
                System.out.println(list);
            }
        }
    }

    void select3(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var sql = "select * from example94";
            try (var ps = session.createPreparedQuery(sql)) {
                tm.execute(transaction -> {
                    try (var rs = ps.execute(transaction)) {
                        for (TsurugiResultEntity entity : rs) {
                            entity.setConvertUtil(CONVERT_UTIL); // set result entity

                            int key = entity.getInt4("key1");
                            String s = entity.getCharacter("date1");
                            LocalDate d = entity.getDate("date1");
                            System.out.printf("key=%d, s=%s, d=%s\n", key, s, d);
                        }
                    }
                });
            }
        }
    }
}
