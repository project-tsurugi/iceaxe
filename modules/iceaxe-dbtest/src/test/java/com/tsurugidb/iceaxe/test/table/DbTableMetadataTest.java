package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.sql.proto.SqlCommon;

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
                    + "  decimal10_2 decimal(10, 2)," //
                    + "  ftext char(10)," //
                    + "  vtext varchar(10)," //
//TODO              + "  fbytes binary(10)," //
//TODO              + "  vbytes varbinary(10)," //
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

        var columnList = metadata.getLowColumnList();
        assertEquals(12, columnList.size());
        int i = 0;
        assertColumn("int4", TgDataType.INT, columnList.get(i++));
        assertColumn("long", TgDataType.LONG, columnList.get(i++));
        assertColumn("float4", TgDataType.FLOAT, columnList.get(i++));
        assertColumn("double8", TgDataType.DOUBLE, columnList.get(i++));
        assertColumn("decimal10_2", TgDataType.DECIMAL, columnList.get(i++));
        assertColumn("ftext", TgDataType.STRING, columnList.get(i++));
        assertColumn("vtext", TgDataType.STRING, columnList.get(i++));
//TODO  assertColumn("fbytes", TgDataType.BYTES, columnList.get(i++));
//TODO  assertColumn("vbytes", TgDataType.BYTES, columnList.get(i++));
        assertColumn("date1", TgDataType.DATE, columnList.get(i++));
        assertColumn("time1", TgDataType.TIME, columnList.get(i++));
        assertColumn("date_time", TgDataType.DATE_TIME, columnList.get(i++));
        assertColumn("offset_time", TgDataType.TIME, columnList.get(i++));
//TODO  assertColumn("offset_time", TgDataType.OFFSET_TIME, columnList.get(i++));
        assertColumn("offset_date_time", TgDataType.DATE_TIME, columnList.get(i++));
//TODO  assertColumn("offset_date_time", TgDataType.OFFSET_DATE_TIME, columnList.get(i++));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column column) {
        assertEquals(name, column.getName());
        assertEquals(type.getLowDataType(), column.getAtomType());
    }

    @Test
    void notFound() throws Exception {
        var session = getSession();
        var metadataOpt = session.findTableMetadata(TEST);
        assertTrue(metadataOpt.isEmpty());
    }
}
