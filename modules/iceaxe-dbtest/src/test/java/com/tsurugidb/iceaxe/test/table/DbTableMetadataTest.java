package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.metadata.TgSqlColumn;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * table metadata test
 */
class DbTableMetadataTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void found() throws Exception {
        var session = getSession();
        {
            var sql = "create table " + TEST //
                    + "(" //
//                  + "  bool boolean," //
                    + "  int4 int," //
                    + "  long bigint," //
                    + "  float4 real," //
                    + "  double8 double," //
                    + "  decimal_ decimal," //
                    + "  decimal10 decimal(10)," //
                    + "  decimal10_2 decimal(10, 2)," //
                    + "  decimal_a decimal(*)," //
                    + "  decimal_a_2 decimal(*, 2)," //
                    + "  ftext char," //
                    + "  ftext10 char(10)," //
                    + "  vtext varchar," //
                    + "  vtext10 varchar(10)," //
                    + "  vtext_a varchar(*)," //
                    + "  fbytes binary," //
                    + "  fbytes10 binary(10)," //
                    + "  vbytes varbinary," //
                    + "  vbytes10 varbinary(10)," //
                    + "  vbytes_a varbinary(*)," //
                    + "  date1 date," //
                    + "  time1 time," //
                    + "  date_time timestamp," //
                    + "  offset_time time with time zone," //
                    + "  offset_date_time timestamp with time zone," //
                    + "  primary key(int4)" //
                    + ")";
            executeDdl(session, sql);
        }

        var metadata = session.findTableMetadata(TEST).get();
        assertNull(metadata.getDatabaseName());
        assertNull(metadata.getSchemaName());
        assertEquals(TEST, metadata.getTableName());

        var columnList = metadata.getColumnList();
        assertEquals(24, columnList.size());
        int i = 0;
        assertColumn("int4", TgDataType.INT, "INT", columnList.get(i++));
        assertColumn("long", TgDataType.LONG, "BIGINT", columnList.get(i++));
        assertColumn("float4", TgDataType.FLOAT, "REAL", columnList.get(i++));
        assertColumn("double8", TgDataType.DOUBLE, "DOUBLE", columnList.get(i++));
        assertColumn("decimal_", TgDataType.DECIMAL, "DECIMAL(38, 0)", columnList.get(i++));
        assertColumn("decimal10", TgDataType.DECIMAL, "DECIMAL(10, 0)", columnList.get(i++));
        assertColumn("decimal10_2", TgDataType.DECIMAL, "DECIMAL(10, 2)", columnList.get(i++));
        assertColumn("decimal_a", TgDataType.DECIMAL, "DECIMAL(38, 0)", columnList.get(i++));
        assertColumn("decimal_a_2", TgDataType.DECIMAL, "DECIMAL(38, 2)", columnList.get(i++));
        assertColumn("ftext", TgDataType.STRING, "CHAR(1)", columnList.get(i++));
        assertColumn("ftext10", TgDataType.STRING, "CHAR(10)", columnList.get(i++));
        assertColumn("vtext", TgDataType.STRING, "VARCHAR(*)", columnList.get(i++));
        assertColumn("vtext10", TgDataType.STRING, "VARCHAR(10)", columnList.get(i++));
        assertColumn("vtext_a", TgDataType.STRING, "VARCHAR(*)", columnList.get(i++));
        assertColumn("fbytes", TgDataType.BYTES, "BINARY(1)", columnList.get(i++));
        assertColumn("fbytes10", TgDataType.BYTES, "BINARY(10)", columnList.get(i++));
        assertColumn("vbytes", TgDataType.BYTES, "VARBINARY(*)", columnList.get(i++));
        assertColumn("vbytes10", TgDataType.BYTES, "VARBINARY(10)", columnList.get(i++));
        assertColumn("vbytes_a", TgDataType.BYTES, "VARBINARY(*)", columnList.get(i++));
        assertColumn("date1", TgDataType.DATE, "DATE", columnList.get(i++));
        assertColumn("time1", TgDataType.TIME, "TIME", columnList.get(i++));
        assertColumn("date_time", TgDataType.DATE_TIME, "TIMESTAMP", columnList.get(i++));
        assertColumn("offset_time", TgDataType.OFFSET_TIME, "TIME WITH TIME ZONE", columnList.get(i++));
        assertColumn("offset_date_time", TgDataType.OFFSET_DATE_TIME, "TIMESTAMP WITH TIME ZONE", columnList.get(i++));
    }

    private static void assertColumn(String name, TgDataType type, String sqlType, TgSqlColumn column) {
        assertEquals(name, column.getName());
        assertEquals(type, column.getDataType());
        assertEquals(sqlType, column.getSqlType());
    }

    @Test
    void notFound() throws Exception {
        var session = getSession();
        var metadataOpt = session.findTableMetadata(TEST);
        assertTrue(metadataOpt.isEmpty());
    }
}
