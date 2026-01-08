/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select join + where test
 */
class DbSelectJoinWhereTest extends DbTestTableTester {

    // table name
    private static final String MASTER = "master";
    private static final String DETAIL = "detail";

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectJoinWhereTest.class);
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
                    + "  m_id1 int," //
                    + "  m_id2 int," //
                    + "  m_name varchar(10)," //
                    + "  primary key(m_id1, m_id2)" //
                    + ")";
            executeDdl(session, sql);
        }
        {
            var sql = "create table " + DETAIL //
                    + "(" //
                    + "  d_id1 int," //
                    + "  d_id2 int," //
                    + "  d_master_id int," // foreign key to MASTER
                    + "  d_memo varchar(100)," //
                    + "  primary key(d_id1, d_id2)" //
                    + ")";
            executeDdl(session, sql);
        }
    }

    private static final List<MasterEntity> MASTER_LIST = List.of( //
            new MasterEntity(1, 1, "a1-1"), //
            new MasterEntity(1, 2, "a1-2"), //
            new MasterEntity(2, 1, "b2-1"), //
            new MasterEntity(2, 2, "b2-2"), //
            new MasterEntity(3, 1, "c3-1"), //
            new MasterEntity(3, 2, "c3-2"));
    private static final Map<Integer, List<MasterEntity>> MASTER_MAP;
    static {
        var map = new HashMap<Integer, List<MasterEntity>>();
        for (var master : MASTER_LIST) {
            var masterId = master.getId1();
            map.computeIfAbsent(masterId, k -> new ArrayList<>()).add(master);
        }
        MASTER_MAP = map;
    }

    private static final List<DetailEntity> DETAIL_LIST = List.of( //
            new DetailEntity(10, 1, 1, "d10-1-a"), //
            new DetailEntity(10, 2, 1, "d10-2-a"), //
            new DetailEntity(10, 3, 1, "d10-3-a"), //
            new DetailEntity(11, 1, 1, "d11-1-a"), //
            new DetailEntity(11, 2, 1, "d11-2-a"), //
            new DetailEntity(20, 1, 2, "d20-1-b"), //
            new DetailEntity(20, 2, 2, "d20-2-b"), //
            new DetailEntity(40, 1, 4, "master nothing1"), //
            new DetailEntity(40, 2, 4, "master nothing2"), //
            new DetailEntity(90, 1, null, "master null1"), //
            new DetailEntity(90, 2, null, "master null2"));
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
        private int id1;
        private int id2;
        private String name;

        public MasterEntity(int id1, int id2, String name) {
            this.id1 = id1;
            this.id2 = id2;
            this.name = name;
        }

        public int getId1() {
            return id1;
        }

        public int getId2() {
            return id2;
        }

        public String getName() {
            return name;
        }
    }

    private static final String MASTER_INSERT_SQL = "insert into " + MASTER //
            + " (m_id1, m_id2, m_name)" //
            + " values(:m_id1, :m_id2, :m_name)";
    private static final TgEntityParameterMapping<MasterEntity> MASTER_MAPPING = TgParameterMapping.of(MasterEntity.class) //
            .addInt("m_id1", MasterEntity::getId1) //
            .addInt("m_id2", MasterEntity::getId2) //
            .addString("m_name", MasterEntity::getName);

    private static class DetailEntity {
        private int id1;
        private int id2;
        private Integer masterId;
        private String memo;

        public DetailEntity(int id1, int id2, Integer masterId, String memo) {
            this.id1 = id1;
            this.id2 = id2;
            this.masterId = masterId;
            this.memo = memo;
        }

        public int getId1() {
            return id1;
        }

        public int getId2() {
            return id2;
        }

        public Integer getMasterId() {
            return masterId;
        }

        public String getMemo() {
            return memo;
        }
    }

    private static final String DETAIL_INSERT_SQL = "insert into " + DETAIL //
            + " (d_id1, d_id2, d_master_id, d_memo)" //
            + " values(:d_id1, :d_id2, :d_master_id, :d_memo)";
    private static final TgEntityParameterMapping<DetailEntity> DETAIL_MAPPING = TgParameterMapping.of(DetailEntity.class) //
            .addInt("d_id1", DetailEntity::getId1) //
            .addInt("d_id2", DetailEntity::getId2) //
            .addInt("d_master_id", DetailEntity::getMasterId) //
            .addString("d_memo", DetailEntity::getMemo);

    @Test
    void simpleJoin() throws Exception {
        simpleJoin("", pair -> true);
    }

    @Test
    void simpleJoinWhereDetail() throws Exception {
        simpleJoin("and d_id2 = 1", pair -> pair.eqDetailId2(1));
    }

    @Test
    void simpleJoinWhereMaster() throws Exception {
        simpleJoin("and m_id2 = 1", pair -> pair.eqMasterId2(1));
    }

    private void simpleJoin(String where, Predicate<MasterDetailPair> filter) throws Exception {
        var sql = "select * from " + DETAIL + " d, " + MASTER + " m\n" //
                + "where m.m_id1 = d.d_master_id\n" //
                + where + "\n" //
                + "order by d_id1, d_id2, m_id1, m_id2";

        List<MasterDetailPair> expectedList = new ArrayList<>();
        for (var detail : DETAIL_LIST) {
            var masterList = MASTER_MAP.get(detail.getMasterId());
            if (masterList != null) {
                for (var master : masterList) {
                    expectedList.add(new MasterDetailPair(master, detail));
                }
            }
        }
        expectedList = expectedList.stream().filter(filter).collect(Collectors.toList());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @Test
    void innerJoin() throws Exception {
        innerJoin("", pair -> true);
    }

    @Test
    void innerJoinWhereDetail() throws Exception {
        innerJoin("where d_id2 = 1", pair -> pair.eqDetailId2(1));
    }

    @Test
    void innerJoinWhereMaster() throws Exception {
        innerJoin("where m_id2 = 1", pair -> pair.eqMasterId2(1));
    }

    private void innerJoin(String where, Predicate<MasterDetailPair> filter) throws Exception {
        var sql = "select * from " + DETAIL + " d\n" //
                + "inner join " + MASTER + " m on m.m_id1 = d.d_master_id\n" //
                + where + "\n" //
                + "order by d_id1, d_id2, m_id1, m_id2";

        List<MasterDetailPair> expectedList = new ArrayList<>();
        for (var detail : DETAIL_LIST) {
            var masterList = MASTER_MAP.get(detail.getMasterId());
            if (masterList != null) {
                for (var master : masterList) {
                    expectedList.add(new MasterDetailPair(master, detail));
                }
            }
        }
        expectedList = expectedList.stream().filter(filter).collect(Collectors.toList());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @Test
    void leftJoin() throws Exception {
        leftJoin("", pair -> true);
    }

    @Test
    void leftJoinWhereDetail() throws Exception {
        leftJoin("where d_id2 = 1", pair -> pair.eqDetailId2(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "d_id1", "d_id2", "d_memo" })
    void leftJoinWhereDetailIsNull(String column) throws Exception {
        leftJoin("where " + column + " is null", pair -> pair.isDetailNull());
    }

    @Test
    void leftJoinWhereMaster() throws Exception {
        leftJoin("where m_id2 = 1", pair -> pair.eqMasterId2(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "m_id1", "m_id2", "m_name" })
    void leftJoinWhereMasterIsNull(String column) throws Exception {
        leftJoin("where " + column + " is null", pair -> pair.isMasterNull());
    }

    private void leftJoin(String where, Predicate<MasterDetailPair> filter) throws Exception {
        var sql = "select * from " + DETAIL + " d\n" //
                + "left join " + MASTER + " m on m.m_id1 = d.d_master_id\n" //
                + where + "\n" //
                + "order by d_id1, d_id2, m_id1, m_id2";

        List<MasterDetailPair> expectedList = new ArrayList<>();
        for (var detail : DETAIL_LIST) {
            var masterList = MASTER_MAP.get(detail.getMasterId());
            if (masterList != null) {
                for (var master : masterList) {
                    expectedList.add(new MasterDetailPair(master, detail));
                }
            } else {
                expectedList.add(new MasterDetailPair(null, detail));
            }
        }
        expectedList = expectedList.stream().filter(filter).collect(Collectors.toList());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @Test
    void rightJoin() throws Exception {
        rightJoin("", pair -> true);
    }

    @Test
    void rightJoinWhereDetail() throws Exception {
        rightJoin("where d_id2 = 1", pair -> pair.eqDetailId2(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "d_id1", "d_id2", "d_memo" })
    void rightJoinWhereDetailIsNull(String column) throws Exception {
        rightJoin("where " + column + " is null", pair -> pair.isDetailNull());
    }

    @Test
    void rightJoinWhereMaster() throws Exception {
        rightJoin("where m_id2 = 1", pair -> pair.eqMasterId2(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "m_id1", "m_id2", "m_name" })
    void rightJoinWhereMasterIsNull(String column) throws Exception {
        rightJoin("where " + column + " is null", pair -> pair.isMasterNull());
    }

    private void rightJoin(String where, Predicate<MasterDetailPair> filter) throws Exception {
        var sql = "select * from " + DETAIL + " d\n" //
                + "right join " + MASTER + " m on m.m_id1 = d.d_master_id\n" //
                + where + "\n" //
                + "order by d_id1, d_id2, m_id1, m_id2";

        List<MasterDetailPair> expectedList = new ArrayList<>();
        for (var master : MASTER_LIST) {
            var detailList = DETAIL_MAP.get(master.getId1());
            if (detailList != null) {
                for (var detail : detailList) {
                    expectedList.add(new MasterDetailPair(master, detail));
                }
            } else {
                expectedList.add(new MasterDetailPair(master, null));
            }
        }
        expectedList = expectedList.stream().filter(filter).collect(Collectors.toList());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @Test
    void fullJoin() throws Exception {
        fullJoin("", pair -> true);
    }

    @Test
    void fullJoinWhereDetail() throws Exception {
        fullJoin("where d_id2 = 1", pair -> pair.eqDetailId2(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "d_id1", "d_id2", "d_memo" })
    void fullJoinWhereDetailIsNull(String column) throws Exception {
        fullJoin("where " + column + " is null", pair -> pair.isDetailNull());
    }

    @Test
    void fullJoinWhereMaster() throws Exception {
        fullJoin("where m_id2 = 1", pair -> pair.eqMasterId2(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "m_id1", "m_id2", "m_name" })
    void fullJoinWhereMasterIsNull(String column) throws Exception {
        fullJoin("where " + column + " is null", pair -> pair.isMasterNull());
    }

    private void fullJoin(String where, Predicate<MasterDetailPair> filter) throws Exception {
        var sql = "select * from " + DETAIL + " d\n" //
                + "full join " + MASTER + " m on m.m_id1 = d.d_master_id\n" //
                + where + "\n" //
                + "order by d_id1, d_id2, m_id1, m_id2";

        List<MasterDetailPair> expectedList = new ArrayList<>();
        for (var detail : DETAIL_LIST) {
            var masterList = MASTER_MAP.get(detail.getMasterId());
            if (masterList != null) {
                for (var master : masterList) {
                    expectedList.add(new MasterDetailPair(master, detail));
                }
            } else {
                expectedList.add(new MasterDetailPair(null, detail));
            }
        }
        for (var master : MASTER_LIST) {
            var detailList = DETAIL_MAP.get(master.getId1());
            if (detailList == null) {
                expectedList.add(new MasterDetailPair(master, null));
            }
        }
        expectedList = expectedList.stream().filter(filter).collect(Collectors.toList());

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
        crossJoin(join, "", pair -> true);
    }

    @ParameterizedTest
    @ValueSource(strings = { "cross join", "," })
    void crossJoinWhereDetail(String join) throws Exception {
        crossJoin(join, "where d_id2 = 1", pair -> pair.eqDetailId2(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "cross join", "," })
    void crossJoinWhereMaster(String join) throws Exception {
        crossJoin(join, "where m_id2 = 1", pair -> pair.eqMasterId2(1));
    }

    private void crossJoin(String join, String where, Predicate<MasterDetailPair> filter) throws Exception {
        var sql = "select * from " + DETAIL + " d\n" //
                + join + " " + MASTER + "\n" //
                + where + "\n" //
                + "order by d_id1, d_id2, m_id1, m_id2";

        List<MasterDetailPair> expectedList = new ArrayList<>();
        for (var master : MASTER_LIST) {
            for (var detail : DETAIL_LIST) {
                expectedList.add(new MasterDetailPair(master, detail));
            }
        }
        expectedList = expectedList.stream().filter(filter).collect(Collectors.toList());

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

        public boolean eqMasterId2(int id2) {
            if (master == null) {
                return false;
            }
            return master.id2 == id2;
        }

        public boolean isMasterNull() {
            return master == null;
        }

        public boolean eqDetailId2(int id2) {
            if (detail == null) {
                return false;
            }
            return detail.id2 == id2;
        }

        public boolean isDetailNull() {
            return detail == null;
        }

        @Override
        public int compareTo(MasterDetailPair that) {
            if (this.detail == null && that.detail == null) {
                int c = Integer.compare(this.master.getId1(), that.master.getId1());
                if (c != 0) {
                    return c;
                }
                return Integer.compare(this.master.getId2(), that.master.getId2());
            }
            if (this.detail == null) {
                return -1;
            }
            if (that.detail == null) {
                return 1;
            }

            int c = Integer.compare(this.detail.getId1(), that.detail.getId1());
            if (c != 0) {
                return c;
            }
            c = Integer.compare(this.detail.getId2(), that.detail.getId2());
            if (c != 0) {
                return c;
            }
            c = Integer.compare(this.master.getId1(), that.master.getId1());
            if (c != 0) {
                return c;
            }
            return Integer.compare(this.master.getId2(), that.master.getId2());
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
                assertEquals(detail.getId1(), actual.getInt("d_id1"));
                assertEquals(detail.getId2(), actual.getInt("d_id2"));
                assertEquals(detail.getMasterId(), actual.getIntOrNull("d_master_id"));
                assertEquals(detail.getMemo(), actual.getString("d_memo"));
            } else {
                assertNull(actual.getIntOrNull("d_id1"));
                assertNull(actual.getIntOrNull("d_id2"));
                assertNull(actual.getIntOrNull("d_master_id"));
                assertNull(actual.getStringOrNull("d_memo"));
            }
            var master = pair.master;
            if (master != null) {
                assertEquals(master.getId1(), actual.getInt("m_id1"));
                assertEquals(master.getId2(), actual.getInt("m_id2"));
                assertEquals(master.getName(), actual.getString("m_name"));
            } else {
                assertNull(actual.getIntOrNull("m_id1"));
                assertNull(actual.getIntOrNull("m_id2"));
                assertNull(actual.getStringOrNull("m_name"));
            }
        }
    }
}
