package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

/**
 * table metadata test
 */
class DbTableMetadataTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void found() throws IOException {
        var session = getSession();
        {
            var sql = "create table " + TEST //
                    + "(" //
                    + "  foo int," //
                    + "  bar bigint," //
                    + "  zzz varchar(10)," //
                    + "  primary key(foo)" //
                    + ")";
            executeDdl(session, sql);
        }

        var metadata = session.findTableMetadata(TEST).get();
        assertNull(metadata.getDatabaseName());
        assertNull(metadata.getSchemaName());
        assertEquals(TEST, metadata.getTableName());

        var columnList = metadata.getLowColumnList();
        assertEquals(3, columnList.size());
        var foo = columnList.get(0);
        assertEquals("foo", foo.getName());
        assertEquals(AtomType.INT4, foo.getAtomType());
        var bar = columnList.get(1);
        assertEquals("bar", bar.getName());
        assertEquals(AtomType.INT8, bar.getAtomType());
        var zzz = columnList.get(2);
        assertEquals("zzz", zzz.getName());
        assertEquals(AtomType.CHARACTER, zzz.getAtomType());
    }

    @Test
    void notFound() throws IOException {
        var session = getSession();
        var metadataOpt = session.findTableMetadata(TEST);
        assertTrue(metadataOpt.isEmpty());
    }
}
