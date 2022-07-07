package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;

/**
 * example to domain type
 */
public class Example93DomainType {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        var info = TgSessionInfo.of("user", "password");
        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            var entity = new Example93Entity2();
            createTable(session, tm);
            switch (1) {
            case 1:
                insert1(session, tm, entity);
                break;
            case 2:
                insert2(session, tm, entity);
                break;
            case 3:
                insert3(session, tm, entity);
                break;
            }

            select1(session, tm);
            select2(session, tm);
            select3(session, tm);
        }
    }

    void createTable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "create table example93 (" //
                + "key1 int," //
                + "type varchar," // @see ExampleType
                + "primary key(key1)" //
                + ")";
        try (var ps = session.createPreparedStatement(sql)) {
            ps.executeAndGetCount(tm);
        }
    }

    /**
     * example to domain type
     */
    enum ExampleType {
        A, B, C;
    }

    static class Example93Entity1 {
        private int key;
        private ExampleType type;

        public void setKey(int key) {
            this.key = key;
        }

        public int getKey() {
            return this.key;
        }

        public void setType(ExampleType type) {
            this.type = type;
        }

        public ExampleType getType() {
            return this.type;
        }
    }

    static class Example93Entity2 extends Example93Entity1 {

        public void setTypeAsString(String value) {
            ExampleType type = (value != null) ? ExampleType.valueOf(value) : null;
            setType(type);
        }

        public String getTypeAsString() {
            var type = getType();
            return (type != null) ? type.name() : null;
        }
    }

    void insert1(TsurugiSession session, TsurugiTransactionManager tm, Example93Entity2 entity) throws IOException {
        var sql = "insert into example93 (key1, type) values(:key, :type)";
        var parameterMapping = TgParameterMapping.of(Example93Entity2.class) //
                .int4("key", Example93Entity2::getKey) //
                .character("type", Example93Entity2::getTypeAsString); // getter as String
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            ps.executeAndGetCount(tm, entity);
        }
    }

    void insert2(TsurugiSession session, TsurugiTransactionManager tm, Example93Entity1 entity) throws IOException {
        var key = TgVariable.ofInt4("key");
        var type = new ExampleTypeVariable("type"); // custom TgVariable
        var sql = "insert into example93 (key1, type) values(" + key + ", " + type + ")";
        var parameterMapping = TgParameterMapping.of(Example93Entity1.class) //
                .add(key, Example93Entity1::getKey) //
                .add(type, Example93Entity1::getType);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            ps.executeAndGetCount(tm, entity);
        }
    }

    static class ExampleTypeVariable extends TgVariable<ExampleType> {

        public ExampleTypeVariable(String name) {
            super(name, TgDataType.CHARACTER); // テーブルのデータ型
        }

        @Override
        public TgParameter bind(ExampleType value) {
            String v = (value != null) ? value.name() : null; // テーブルのデータ型へ変換
            return TgParameter.of(name(), v);
        }

        @Override
        public TgVariable<ExampleType> copy(String name) {
            return new ExampleTypeVariable(name);
        }
    }

    void insert3(TsurugiSession session, TsurugiTransactionManager tm, Example93Entity1 entity) throws IOException {
        var sql = "insert into example93 (key1, type) values(:key, :type)";
        var parameterMapping = TgParameterMapping.of(Example93Entity1.class) //
                .int4("key", Example93Entity1::getKey) //
                .character("type", Example93Entity1::getType, ExampleType::name); // getter with converter
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            ps.executeAndGetCount(tm, entity);
        }
    }

    void select1(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from example93";
        var resultMapping = TgResultMapping.of(Example93Entity1::new) //
                .int4(Example93Entity1::setKey) //
                .add(this::setEntityType); // converter
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            var list = ps.executeAndGetList(tm);
            System.out.println(list);
        }
    }

    private void setEntityType(Example93Entity1 entity, TsurugiResultRecord record) throws IOException {
        String value = record.nextCharacterOrNull();
        ExampleType type = (value != null) ? ExampleType.valueOf(value) : null;
        entity.setType(type);
    }

    void select2(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from example93";
        var resultMapping = TgResultMapping.of(Example93Entity2::new) //
                .int4(Example93Entity2::setKey) //
                .character(Example93Entity2::setTypeAsString); // setter as String
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            var list = ps.executeAndGetList(tm);
            System.out.println(list);
        }
    }

    void select3(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from example93";
        var resultMapping = TgResultMapping.of(Example93Entity1::new) //
                .int4(Example93Entity1::setKey) //
                .character(Example93Entity1::setType, ExampleType::valueOf); // setter with converter
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            var list = ps.executeAndGetList(tm);
            System.out.println(list);
        }
    }
}
