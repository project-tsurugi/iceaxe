package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * example to domain type
 */
public class Example93DomainType {

    void main() throws IOException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            createTable(tm);

            var entity = new Example93Entity2();
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

    void createTable(TsurugiTransactionManager tm) throws IOException {
        var sql = "create table example93 (" //
                + "key1 int," //
                + "type varchar," // @see ExampleType
                + "primary key(key1)" //
                + ")";
        tm.executeDdl(sql);
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
                .addInt("key", Example93Entity2::getKey) //
                .addString("type", Example93Entity2::getTypeAsString); // getter as String
        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.executeAndGetCount(ps, entity);
        }
    }

    void insert2(TsurugiSession session, TsurugiTransactionManager tm, Example93Entity1 entity) throws IOException {
        var key = TgBindVariable.ofInt("key");
        var type = new ExampleTypeVariable("type"); // custom TgVariable
        var sql = "insert into example93 (key1, type) values(" + key + ", " + type + ")";
        var parameterMapping = TgParameterMapping.of(Example93Entity1.class) //
                .add(key, Example93Entity1::getKey) //
                .add(type, Example93Entity1::getType);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.executeAndGetCount(ps, entity);
        }
    }

    static class ExampleTypeVariable extends TgBindVariable<ExampleType> {

        public ExampleTypeVariable(String name) {
            super(name, TgDataType.STRING); // テーブルのデータ型
        }

        @Override
        public TgBindParameter bind(ExampleType value) {
            String v = (value != null) ? value.name() : null; // テーブルのデータ型へ変換
            return TgBindParameter.of(name(), v);
        }

        @Override
        public TgBindVariable<ExampleType> clone(String name) {
            return new ExampleTypeVariable(name);
        }
    }

    void insert3(TsurugiSession session, TsurugiTransactionManager tm, Example93Entity1 entity) throws IOException {
        var sql = "insert into example93 (key1, type) values(:key, :type)";
        var parameterMapping = TgParameterMapping.of(Example93Entity1.class) //
                .addInt("key", Example93Entity1::getKey) //
                .addString("type", Example93Entity1::getType, ExampleType::name); // getter with converter
        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.executeAndGetCount(ps, entity);
        }
    }

    void select1(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from example93";
        var resultMapping = TgResultMapping.of(Example93Entity1::new) //
                .addInt(Example93Entity1::setKey) //
                .add(this::setEntityType); // converter
        try (var ps = session.createQuery(sql, resultMapping)) {
            var list = tm.executeAndGetList(ps);
            System.out.println(list);
        }
    }

    private void setEntityType(Example93Entity1 entity, TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        String value = record.nextStringOrNull();
        ExampleType type = (value != null) ? ExampleType.valueOf(value) : null;
        entity.setType(type);
    }

    void select2(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from example93";
        var resultMapping = TgResultMapping.of(Example93Entity2::new) //
                .addInt(Example93Entity2::setKey) //
                .addString(Example93Entity2::setTypeAsString); // setter as String
        try (var ps = session.createQuery(sql, resultMapping)) {
            var list = tm.executeAndGetList(ps);
            System.out.println(list);
        }
    }

    void select3(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from example93";
        var resultMapping = TgResultMapping.of(Example93Entity1::new) //
                .addInt(Example93Entity1::setKey) //
                .addString(Example93Entity1::setType, ExampleType::valueOf); // setter with converter
        try (var ps = session.createQuery(sql, resultMapping)) {
            var list = tm.executeAndGetList(ps);
            System.out.println(list);
        }
    }
}
