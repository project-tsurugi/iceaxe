package com.example.iceaxe;

import java.io.IOException;

import com.tsurugi.iceaxe.result.TsurugiResultRecord;
import com.tsurugi.iceaxe.statement.TgParameter;
import com.tsurugi.iceaxe.statement.TgVariable;

/**
 * example entity for 'TEST' table
 */
public class TestEntity {
    public static final String INSERT_SQL = "insert into TEST values(:foo, :bar, :zzz)";

    public static final TgVariable VARIABLE = TgVariable.of().int4("foo").int8("bar").character("zzz");
//  public static final TgVariable VARIABLE = TgVariable.of().set("foo", TgDataType.INT4).set("bar", TgDataType.INT8).set("zzz", TgDataType.CHARACTER);
//  public static final TgVariable VARIABLE = TgVariable.of().set("foo", int.class).set("bar", long.class).set("zzz", String.class);

    public static TgParameter toParameter(TestEntity entity) {
        return TgParameter.of().set("foo", entity.getFoo()).set("bar", entity.getBar()).set("zzz", entity.getZzz());
    }

//  public TgParameter toParameter() {
//      return TgParameter.of().set("foo", foo).set("bar", bar).set("zzz", zzz);
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

    public static TestEntity of(TsurugiResultRecord record) throws IOException {
        var entity = new TestEntity();
        // FIXME カラム名は大文字にすべき？
        entity.setFoo(record.getInt4OrNull("foo"));
//TODO        entity.setBar(record.getInt8OrNull("bar"));
//TODO        entity.setZzz(record.getCharacterOrNull("zzz"));
        return entity;
    }
}
