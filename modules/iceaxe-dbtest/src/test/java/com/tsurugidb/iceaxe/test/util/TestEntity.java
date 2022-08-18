package com.tsurugidb.iceaxe.test.util;

import java.util.Objects;

/**
 * example entity for 'test' table
 */
public class TestEntity {

    private Integer foo;
    private Long bar;
    private String zzz;

    public TestEntity() {
    }

    public TestEntity(int foo, long bar, String zzz) {
        this.foo = foo;
        this.bar = bar;
        this.zzz = zzz;
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

    @Override
    public int hashCode() {
        return Objects.hash(bar, foo, zzz);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        var other = (TestEntity) obj;
        return Objects.equals(bar, other.bar) && Objects.equals(foo, other.foo) && Objects.equals(zzz, other.zzz);
    }

    @Override
    public String toString() {
        return "TestEntity{foo=" + foo + ", bar=" + bar + ", zzz=" + zzz + "}";
    }
}
