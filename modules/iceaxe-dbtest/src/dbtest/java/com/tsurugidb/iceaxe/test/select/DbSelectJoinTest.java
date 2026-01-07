package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select join test
 */
class DbSelectJoinTest extends DbTestTableTester {

    // table name
    private static final String MASTER = "master";
    private static final String DETAIL = "detail";

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectJoinTest.class);
        logInitStart(LOG, info);

        dropMasterDetail();
        createMasterDetail();
        insertMasterDetail();

        logInitEnd(LOG, info);
    }

    private static void dropMasterDetail() throws IOException, InterruptedException {
        dropTable(MASTER);
        dropTable(DETAIL);
    }

    private static void createMasterDetail() throws IOException, InterruptedException {
        var session = getSession();
        {
            var sql = "create table " + MASTER //
                    + "(" //
                    + "  m_id int," //
                    + "  m_name varchar(10)," //
                    + "  primary key(m_id)" //
                    + ")";
            executeDdl(session, sql);
        }
        {
            var sql = "create table " + DETAIL //
                    + "(" //
                    + "  d_id bigint," //
                    + "  d_master_id int," // foreign key to MASTER
                    + "  d_memo varchar(100)," //
                    + "  primary key(d_id)" //
                    + ")";
            executeDdl(session, sql);
        }
    }

    private static final List<MasterEntity> MASTER_LIST = List.of( //
            new MasterEntity(1, "aaa"), //
            new MasterEntity(2, "bbb"), //
            new MasterEntity(3, "ccc"));
    private static final Map<Integer, MasterEntity> MASTER_MAP = MASTER_LIST.stream().collect(Collectors.toMap(e -> e.getId(), e -> e));

    private static final List<DetailEntity> DETAIL_LIST = List.of( //
            new DetailEntity(11, 1, "a1"), //
            new DetailEntity(12, 1, "a2"), //
            new DetailEntity(13, 1, "a3"), //
            new DetailEntity(21, 2, "b1"), //
            new DetailEntity(40, 4, "master nothing"), //
            new DetailEntity(90, null, "master null"));
    private static final Map<Integer, List<DetailEntity>> DETAIL_MAP;
    static {
        var map = new HashMap<Integer, List<DetailEntity>>();
        for (var detail : DETAIL_LIST) {
            var masterId = detail.getMasterId();
            map.computeIfAbsent(masterId, k -> new ArrayList<>()).add(detail);
        }
        DETAIL_MAP = map;
    }

    private static void insertMasterDetail() throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        tm.execute(transaction -> {
            try (var ps = session.createStatement(MASTER_INSERT_SQL, MASTER_MAPPING)) {
                for (var entity : MASTER_LIST) {
                    transaction.executeAndGetCount(ps, entity);
                }
            }
            try (var ps = session.createStatement(DETAIL_INSERT_SQL, DETAIL_MAPPING)) {
                for (var entity : DETAIL_LIST) {
                    transaction.executeAndGetCount(ps, entity);
                }
            }
        });
    }

    private static class MasterEntity {
        private int id;
        private String name;

        public MasterEntity(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    private static final String MASTER_INSERT_SQL = "insert into " + MASTER //
            + " (m_id, m_name)" //
            + " values(:m_id, :m_name)";
    private static final TgEntityParameterMapping<MasterEntity> MASTER_MAPPING = TgParameterMapping.of(MasterEntity.class) //
            .addInt("m_id", MasterEntity::getId) //
            .addString("m_name", MasterEntity::getName);

    private static class DetailEntity {
        private long id;
        private Integer masterId;
        private String memo;

        public DetailEntity(long id, Integer masterId, String memo) {
            this.id = id;
            this.masterId = masterId;
            this.memo = memo;
        }

        public long getId() {
            return id;
        }

        public Integer getMasterId() {
            return masterId;
        }

        public String getMemo() {
            return memo;
        }
    }

    private static final String DETAIL_INSERT_SQL = "insert into " + DETAIL //
            + " (d_id, d_master_id, d_memo)" //
            + " values(:d_id, :d_master_id, :d_memo)";
    private static final TgEntityParameterMapping<DetailEntity> DETAIL_MAPPING = TgParameterMapping.of(DetailEntity.class) //
            .addLong("d_id", DetailEntity::getId) //
            .addInt("d_master_id", DetailEntity::getMasterId) //
            .addString("d_memo", DetailEntity::getMemo);

    @ParameterizedTest
    @ValueSource(strings = { "*", "d.*, m.*" })
    void simpleJoin(String select) throws Exception {
        var sql = "select " + select + " from " + DETAIL + " d, " + MASTER + " m\n" //
                + "where m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var detail : DETAIL_LIST) {
            var master = MASTER_MAP.get(detail.getMasterId());
            if (master != null) {
                expectedList.add(new MasterDetailPair(master, detail));
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @Test
    void simpleJoinSameAlias() throws Exception {
        var sql = "select * from " + DETAIL + " a\n" //
                + ", " + MASTER + " a";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(MASTER_LIST.size() * DETAIL_LIST.size(), list.size());
        }
    }

    @Test
    void simpleJoinSameAliasError() throws Exception {
        var sql = "select a.* from " + DETAIL + " a\n" //
                + ", " + MASTER + " a";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
            String message = e.getMessage();
            assertContains("compile failed with error:relation_ambiguous", message);
            assertContains("'a'", message);
        }
    }

    @Test
    void simpleJoinSameTableSameAlias() throws Exception {
        var sql = "select * from " + MASTER + " a\n" //
                + ", " + MASTER + " a";

        var expectedList = new ArrayList<MasterEntity>();
        for (@SuppressWarnings("unused")
        var master1 : MASTER_LIST) {
            for (var master2 : MASTER_LIST) {
                // TODO 重複カラムがあるとき、それらが区別できるべき？
                expectedList.add(master2);
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(expectedList.size(), list.size());
            for (var actual : list) {
                findAndRemove(expectedList, actual);
            }
        }
    }

    @Test
    void simpleJoinSameTable() throws Exception {
        var sql = "select m1.* from " + MASTER + " m1\n" //
                + ", " + MASTER + " m2";

        var expectedList = new ArrayList<MasterEntity>();
        for (var master1 : MASTER_LIST) {
            for (@SuppressWarnings("unused")
            var master2 : MASTER_LIST) {
                expectedList.add(master1);
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(expectedList.size(), list.size());
            for (var actual : list) {
                findAndRemove(expectedList, actual);
            }
        }
    }

    private void findAndRemove(List<MasterEntity> expectedList, TsurugiResultEntity actual) {
        for (var i = expectedList.iterator(); i.hasNext();) {
            var expected = i.next();
            if (expected.getId() == actual.getInt("m_id") && expected.getName().equals(actual.getString("m_name"))) {
                i.remove();
                return;
            }
        }
        fail("not found " + actual + " in " + expectedList);
    }

    @Test
    void simpleJoinSameTableCount() throws Exception {
        var sql = "select count(*) from " + MASTER + " m1\n" //
                + ", " + MASTER + " m2";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, TgResultMapping.ofSingle(int.class))) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(MASTER_LIST.size() * MASTER_LIST.size(), count);
        }
    }

    @Test
    void innerJoin() throws Exception {
        innerJoin("*");
    }

    @Test
    void innerJoinAlias() throws Exception {
        innerJoin("d.*, m.*");
    }

    private void innerJoin(String select) throws Exception {
        var sql = "select " + select + " from " + DETAIL + " d\n" //
                + "inner join " + MASTER + " m on m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var detail : DETAIL_LIST) {
            var master = MASTER_MAP.get(detail.getMasterId());
            if (master != null) {
                expectedList.add(new MasterDetailPair(master, detail));
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "left join", "left outer join" })
    void leftJoin(String join) throws Exception {
        leftJoin("*", join);
    }

    @Test
    void leftJoinAlias() throws Exception {
        leftJoin("d.*, m.*", "left join");
    }

    private void leftJoin(String select, String join) throws Exception {
        var sql = "select " + select + " from " + DETAIL + " d\n" //
                + join + " " + MASTER + " m on m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var detail : DETAIL_LIST) {
            var master = MASTER_MAP.get(detail.getMasterId());
            expectedList.add(new MasterDetailPair(master, detail));
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "right join", "right outer join" })
    void rightJoin(String join) throws Exception {
        rightJoin("*", join);
    }

    @Test
    void rightJoinAlias() throws Exception {
        rightJoin("d.*, m.*", "right join");
    }

    private void rightJoin(String select, String join) throws Exception {
        var sql = "select " + select + " from " + DETAIL + " d\n" //
                + join + " " + MASTER + " m on m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var master : MASTER_LIST) {
            var detailList = DETAIL_MAP.get(master.getId());
            if (detailList != null) {
                for (var detail : detailList) {
                    expectedList.add(new MasterDetailPair(master, detail));
                }
            } else {
                expectedList.add(new MasterDetailPair(master, null));
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "full join", "full outer join" })
    void fullJoin(String join) throws Exception {
        fullJoin("*", join);
    }

    @Test
    void fullJoinAlias() throws Exception {
        fullJoin("d.*, m.*", "full join");
    }

    private void fullJoin(String select, String join) throws Exception {
        var sql = "select " + select + " from " + DETAIL + " d\n" //
                + join + " " + MASTER + " m on m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var detail : DETAIL_LIST) {
            var master = MASTER_MAP.get(detail.getMasterId());
            expectedList.add(new MasterDetailPair(master, detail));
        }
        for (var master : MASTER_LIST) {
            var detailList = DETAIL_MAP.get(master.getId());
            if (detailList == null) {
                expectedList.add(new MasterDetailPair(master, null));
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "cross join", "," })
    void crossJoin(String join) throws Exception {
        crossJoin("*", join);
    }

    @ParameterizedTest
    @ValueSource(strings = { "cross join", "," })
    void crossJoinAlias(String join) throws Exception {
        crossJoin("d.*, m.*", join);
    }

    private void crossJoin(String select, String join) throws Exception {
        var sql = "select " + select + " from " + DETAIL + " d\n" //
                + join + " " + MASTER + " m\n" //
                + "order by d_id, m_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var master : MASTER_LIST) {
            for (var detail : DETAIL_LIST) {
                expectedList.add(new MasterDetailPair(master, detail));
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    private static class MasterDetailPair implements Comparable<MasterDetailPair> {
        private MasterEntity master;
        private DetailEntity detail;

        public MasterDetailPair(MasterEntity master, DetailEntity detail) {
            this.master = master;
            this.detail = detail;
        }

        @Override
        public int compareTo(MasterDetailPair that) {
            if (this.detail == null && that.detail == null) {
                return Integer.compare(this.master.getId(), that.master.getId());
            }
            if (this.detail == null) {
                return -1;
            }
            if (that.detail == null) {
                return 1;
            }

            return Long.compare(this.detail.getId(), that.detail.getId());
        }
    }

    private void assertEqualsMasterDetail(List<MasterDetailPair> expectedList, List<TsurugiResultEntity> actualList) {
        Collections.sort(expectedList);
        assertEquals(expectedList.size(), actualList.size());
        int i = 0;
        for (var pair : expectedList) {
            var actual = actualList.get(i++);
            var detail = pair.detail;
            if (detail != null) {
                assertEquals(detail.getId(), actual.getLong("d_id"));
                assertEquals(detail.getMasterId(), actual.getIntOrNull("d_master_id"));
                assertEquals(detail.getMemo(), actual.getString("d_memo"));
            } else {
                assertNull(actual.getLongOrNull("d_id"));
                assertNull(actual.getIntOrNull("d_master_id"));
                assertNull(actual.getStringOrNull("d_memo"));
            }
            var master = pair.master;
            if (master != null) {
                assertEquals(master.getId(), actual.getInt("m_id"));
                assertEquals(master.getName(), actual.getString("m_name"));
            } else {
                assertNull(actual.getIntOrNull("m_id"));
                assertNull(actual.getStringOrNull("m_name"));
            }
        }
    }
}
