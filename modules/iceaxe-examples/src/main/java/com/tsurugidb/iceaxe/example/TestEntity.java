/*
 * Copyright 2023-2025 Project Tsurugi.
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

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * example entity for 'TEST' table
 */
public class TestEntity {
    public static final String INSERT_SQL = "insert into TEST values(:foo, :bar, :zzz)";

    public static final TgBindVariables VARIABLES = TgBindVariables.of().addInt("foo").addLong("bar").addString("zzz");
    static final TgBindVariables VARIABLE1 = TgBindVariables.of().add("foo", TgDataType.INT).add("bar", TgDataType.LONG).add("zzz", TgDataType.STRING);
    static final TgBindVariables VARIABLE2 = TgBindVariables.of().add("foo", int.class).add("bar", long.class).add("zzz", String.class);

    public static TgBindParameters toParameter(TestEntity entity) {
        return TgBindParameters.of().add("foo", entity.getFoo()).add("bar", entity.getBar()).add("zzz", entity.getZzz());
    }

    public TgBindParameters toParameter2() {
        return TgBindParameters.of().add("foo", this.foo).add("bar", this.bar).add("zzz", this.zzz);
    }

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

    @Override
    public String toString() {
        return "TestEntity(foo=" + foo + ", bar=" + bar + ", zzz=" + zzz + ")";
    }

    public static TestEntity of(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        var entity = new TestEntity();
        entity.setFoo(record.getIntOrNull("foo"));
        entity.setBar(record.getLongOrNull("bar"));
        entity.setZzz(record.getStringOrNull("zzz"));
        return entity;
    }
}
