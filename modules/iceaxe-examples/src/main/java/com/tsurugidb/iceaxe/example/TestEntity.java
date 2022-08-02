package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;

/**
 * example entity for 'TEST' table
 */
public class TestEntity {
    public static final String INSERT_SQL = "insert into TEST values(:foo, :bar, :zzz)";

    public static final TgVariableList VARIABLE = TgVariableList.of().int4("foo").int8("bar").character("zzz");
//  public static final TgVariableList VARIABLE = TgVariableList.of().add("foo", TgDataType.INT4).add("bar", TgDataType.INT8).add("zzz", TgDataType.CHARACTER);
//  public static final TgVariableList VARIABLE = TgVariableList.of().add("foo", int.class).add("bar", long.class).add("zzz", String.class);

    public static TgParameterList toParameter(TestEntity entity) {
        return TgParameterList.of().add("foo", entity.getFoo()).add("bar", entity.getBar()).add("zzz", entity.getZzz());
    }

//  public TgParameterList toParameter() {
//      return TgParameterList.of().add("foo", foo).add("bar", bar).add("zzz", zzz);
//  }

    private Integer foo;
    private Long bar;
    private String zzz;

    public TestEntity() {
    }

    public TestEntity(Integer foo, Long bar, String zzz) {
        this.foo = foo;
        this.bar = bar;
        this.zzz = zzz;
    }

    public Integer getFoo() {
        return foo;
    }

    public void setFoo(Integer foo) {
        this.foo = foo;
    }

    public Long getBar() {
        return bar;
    }

    public void setBar(Long bar) {
        this.bar = bar;
    }

    public String getZzz() {
        return zzz;
    }

    public void setZzz(String zzz) {
        this.zzz = zzz;
    }

    public static TestEntity of(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        var entity = new TestEntity();
        entity.setFoo(record.getInt4OrNull("foo"));
        entity.setBar(record.getInt8OrNull("bar"));
        entity.setZzz(record.getCharacterOrNull("zzz"));
        return entity;
    }
}
