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
package com.tsurugidb.iceaxe.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Empty;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.Column;

class TgSqlColumnTest {

    @Test
    void getName() {
        var lowColumn = Column.newBuilder().setName("test").build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("test", column.getName());
    }

    @Test
    void getDataType() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.INT4).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals(TgDataType.INT, column.getDataType());
    }

    @Test
    void findLength() {
        {
            var lowColumn = Column.newBuilder().build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(Optional.empty(), column.findLength());
        }
        {
            var lowColumn = Column.newBuilder().setLength(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(ArbitraryInt.of(123), column.findLength().get());
        }
        {
            var lowColumn = Column.newBuilder().setArbitraryLength(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(ArbitraryInt.ofArbitrary(), column.findLength().get());
        }
    }

    @Test
    void findPrecision() {
        {
            var lowColumn = Column.newBuilder().build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(Optional.empty(), column.findPrecision());
        }
        {
            var lowColumn = Column.newBuilder().setPrecision(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(ArbitraryInt.of(123), column.findPrecision().get());
        }
        {
            var lowColumn = Column.newBuilder().setArbitraryPrecision(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(ArbitraryInt.ofArbitrary(), column.findPrecision().get());
        }
    }

    @Test
    void findScale() {
        {
            var lowColumn = Column.newBuilder().build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(Optional.empty(), column.findScale());
        }
        {
            var lowColumn = Column.newBuilder().setScale(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(ArbitraryInt.of(123), column.findScale().get());
        }
        {
            var lowColumn = Column.newBuilder().setArbitraryScale(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(ArbitraryInt.ofArbitrary(), column.findScale().get());
        }
    }

    @Test
    void findNullable() {
        {
            var lowColumn = Column.newBuilder().build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(Optional.empty(), column.findNullable());
        }
        {
            var lowColumn = Column.newBuilder().setNullable(true).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(true, column.findNullable().get());
        }
        {
            var lowColumn = Column.newBuilder().setNullable(false).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(false, column.findNullable().get());
        }
    }

    @Test
    void findVarying() {
        {
            var lowColumn = Column.newBuilder().build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(Optional.empty(), column.findVarying());
        }
        {
            var lowColumn = Column.newBuilder().setVarying(true).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(true, column.findVarying().get());
        }
        {
            var lowColumn = Column.newBuilder().setVarying(false).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(false, column.findVarying().get());
        }
    }

    @Test
    void getSqlType_BOOLEAN() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.BOOLEAN).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("BOOLEAN", column.getSqlType());
        assertEquals("BOOLEAN", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_INT4() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.INT4).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("INT", column.getSqlType());
        assertEquals("INT", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_INT8() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.INT8).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("BIGINT", column.getSqlType());
        assertEquals("BIGINT", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_FLOAT4() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.FLOAT4).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("REAL", column.getSqlType());
        assertEquals("REAL", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_FLOAT8() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.FLOAT8).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("DOUBLE", column.getSqlType());
        assertEquals("DOUBLE", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_DECIMAL() {
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.DECIMAL).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("DECIMAL", column.getSqlType());
            assertEquals("DECIMAL", column.getSqlTypeOrAtomTypeName());
        }

        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.DECIMAL).setPrecision(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("DECIMAL(123)", column.getSqlType());
            assertEquals("DECIMAL(123)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.DECIMAL).setPrecision(123).setScale(4).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("DECIMAL(123, 4)", column.getSqlType());
            assertEquals("DECIMAL(123, 4)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.DECIMAL).setPrecision(123).setArbitraryScale(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("DECIMAL(123, *)", column.getSqlType());
            assertEquals("DECIMAL(123, *)", column.getSqlTypeOrAtomTypeName());
        }

        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.DECIMAL).setArbitraryPrecision(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("DECIMAL(*)", column.getSqlType());
            assertEquals("DECIMAL(*)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.DECIMAL).setArbitraryPrecision(Empty.newBuilder().build()).setScale(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("DECIMAL(*, 123)", column.getSqlType());
            assertEquals("DECIMAL(*, 123)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.DECIMAL).setArbitraryPrecision(Empty.newBuilder().build()).setArbitraryScale(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("DECIMAL(*, *)", column.getSqlType());
            assertEquals("DECIMAL(*, *)", column.getSqlTypeOrAtomTypeName());
        }
    }

    @Test
    void getSqlType_CHARACTER() {
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(null, column.getSqlType());
            assertEquals("CHARACTER", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).setLength(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(null, column.getSqlType());
            assertEquals("CHARACTER", column.getSqlTypeOrAtomTypeName());
        }

        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).setVarying(false).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("CHAR", column.getSqlType());
            assertEquals("CHAR", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).setVarying(false).setLength(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("CHAR(123)", column.getSqlType());
            assertEquals("CHAR(123)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).setVarying(false).setArbitraryLength(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("CHAR(*)", column.getSqlType());
            assertEquals("CHAR(*)", column.getSqlTypeOrAtomTypeName());
        }

        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).setVarying(true).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("VARCHAR", column.getSqlType());
            assertEquals("VARCHAR", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).setVarying(true).setLength(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("VARCHAR(123)", column.getSqlType());
            assertEquals("VARCHAR(123)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.CHARACTER).setVarying(true).setArbitraryLength(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("VARCHAR(*)", column.getSqlType());
            assertEquals("VARCHAR(*)", column.getSqlTypeOrAtomTypeName());
        }
    }

    @Test
    void getSqlType_OCTET() {
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(null, column.getSqlType());
            assertEquals("OCTET", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).setLength(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals(null, column.getSqlType());
            assertEquals("OCTET", column.getSqlTypeOrAtomTypeName());
        }

        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).setVarying(false).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("BINARY", column.getSqlType());
            assertEquals("BINARY", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).setVarying(false).setLength(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("BINARY(123)", column.getSqlType());
            assertEquals("BINARY(123)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).setVarying(false).setArbitraryLength(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("BINARY(*)", column.getSqlType());
            assertEquals("BINARY(*)", column.getSqlTypeOrAtomTypeName());
        }

        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).setVarying(true).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("VARBINARY", column.getSqlType());
            assertEquals("VARBINARY", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).setVarying(true).setLength(123).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("VARBINARY(123)", column.getSqlType());
            assertEquals("VARBINARY(123)", column.getSqlTypeOrAtomTypeName());
        }
        {
            var lowColumn = Column.newBuilder().setAtomType(AtomType.OCTET).setVarying(true).setArbitraryLength(Empty.newBuilder().build()).build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("VARBINARY(*)", column.getSqlType());
            assertEquals("VARBINARY(*)", column.getSqlTypeOrAtomTypeName());
        }
    }

    @Test
    void getSqlType_DATE() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.DATE).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("DATE", column.getSqlType());
        assertEquals("DATE", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_TIME() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.TIME_OF_DAY).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("TIME", column.getSqlType());
        assertEquals("TIME", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_TIME_TIMEZONE() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.TIME_OF_DAY_WITH_TIME_ZONE).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("TIME WITH TIME ZONE", column.getSqlType());
        assertEquals("TIME WITH TIME ZONE", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_TIMEPOINT() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.TIME_POINT).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("TIMESTAMP", column.getSqlType());
        assertEquals("TIMESTAMP", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_TIMEPOINT_TIMEZONE() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.TIME_POINT_WITH_TIME_ZONE).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("TIMESTAMP WITH TIME ZONE", column.getSqlType());
        assertEquals("TIMESTAMP WITH TIME ZONE", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_BLOB() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.BLOB).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("BLOB", column.getSqlType());
        assertEquals("BLOB", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getSqlType_CLOB() {
        var lowColumn = Column.newBuilder().setAtomType(AtomType.CLOB).build();
        var column = new TgSqlColumn(lowColumn);

        assertEquals("CLOB", column.getSqlType());
        assertEquals("CLOB", column.getSqlTypeOrAtomTypeName());
    }

    @Test
    void getDescription() {
        {
            var lowColumn = Column.newBuilder().setDescription("test").build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("test", column.getDescription());
        }
        {
            var lowColumn = Column.newBuilder().setDescription("").build();
            var column = new TgSqlColumn(lowColumn);

            assertEquals("", column.getDescription());
        }
        {
            var lowColumn = Column.newBuilder().build();
            var column = new TgSqlColumn(lowColumn);

            assertNull(column.getDescription());
        }
    }
}
