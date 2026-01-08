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
package com.tsurugidb.iceaxe.example;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableBlob;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.mapping.TgSingleResultMapping;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgBlobReference;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * BLOB example
 */
public class Example72Blob {

    public static void main(String... args) throws IOException, InterruptedException {
        var connector = TsurugiConnector.of("ipc:tsurugi");
        try (var session = connector.createSession()) {
            var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            new Example72Blob().main(tm);
        }

        new Example72Blob().largeObjectPathMapping();
    }

    void main(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        tm.executeDdl("drop table if exists blob_example");
        tm.executeDdl("create table blob_example (" //
                + "  pk int primary key," //
                + "  value blob" //
                + ")" //
        );

        switch (0) {
        case 0:
            insert(tm);
            break;
        case 1:
            insertBind(tm);
            break;
        case 2:
            inserFromInputStream(tm);
            break;
        case 3:
            inserFromBytes(tm);
            break;
        case 4:
            insertFromEntity(tm, List.of());
            break;
        case 5:
            insertFromEntityPath(tm, List.of());
            break;
        }

        select(tm);
    }

    // insert

    void insert(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "insert into blob_example values(:pk, :value)";
        var variables = TgBindVariables.of().addInt("pk").addBlob("value");
        var parameterMapping = TgParameterMapping.of(variables);

        try (var ps = tm.getSession().createStatement(sql, parameterMapping)) {
            List<Path> fileList;
            try (var stream = Files.list(Path.of("dir"))) {
                fileList = stream.collect(Collectors.toList());
            }

            tm.execute(transaction -> {
                int i = 0;
                for (Path path : fileList) {
                    var parameter = createInsertParameterForBlob(i++, path);
                    transaction.executeAndGetCountDetail(ps, parameter);
                }
            });
        }
    }

    TgBindParameters createInsertParameterForBlob(int pk, Path path) {
        var blob = TgBlob.of(path);
        return TgBindParameters.of().addInt("pk", pk).addBlob("value", blob);
    }

    TgBindParameters createInsertParameterForPath(int pk, Path path) {
        return TgBindParameters.of().addInt("pk", pk).addBlob("value", path);
    }

    void insertBind(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "insert into blob_example values(:pk, :value)";
        var pk = TgBindVariable.ofInt("pk");
        var value = TgBindVariable.ofBlob("value");
        var parameterMapping = TgParameterMapping.of(pk, value);

        try (var ps = tm.getSession().createStatement(sql, parameterMapping)) {
            List<Path> fileList;
            try (var stream = Files.list(Path.of("dir"))) {
                fileList = stream.collect(Collectors.toList());
            }

            tm.execute(transaction -> {
                int i = 0;
                for (Path path : fileList) {
                    var parameter = TgBindParameters.of(pk.bind(i++), bindForBlob(value, path));
                    transaction.executeAndGetCountDetail(ps, parameter);
                }
            });
        }
    }

    TgBindParameter bindForBlob(TgBindVariableBlob value, Path path) {
        var blob = TgBlob.of(path);
        return value.bind(blob);
    }

    TgBindParameter bindForPath(TgBindVariableBlob value, Path path) {
        return value.bind(path);
    }

    void inserFromInputStream(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "insert into blob_example values(:pk, :value)";
        var variables = TgBindVariables.of().addInt("pk").addBlob("value");
        var parameterMapping = TgParameterMapping.of(variables);

        var objectFactory = IceaxeObjectFactory.getDefaultInstance();

        try (var ps = tm.getSession().createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (int i = 0; i < 10; i++) {
                    try (var is = getInputStream(i)) {
                        try (var blob = objectFactory.createBlob(is, false)) { // temporary file is created internally
                            var parameter = TgBindParameters.of() //
                                    .addInt("pk", i) //
                                    .addBlob("value", blob);
                            transaction.executeAndGetCountDetail(ps, parameter);
                        } // delete temporary file in blob.close()
                    }
                }
                return;
            });
        }
    }

    protected InputStream getInputStream(int pk) {
        return new ByteArrayInputStream(new byte[] { 1, 2, 3 });
    }

    static class ExampleBlobEntity {
        private int pk;
        private TgBlob value;
        private Path path;

        public void setPk(int pk) {
            this.pk = pk;
        }

        public int getPk() {
            return this.pk;
        }

        public void setBlobValue(TgBlob value) {
            this.value = value;
        }

        public TgBlob getBlobValue() {
            return this.value;
        }

        public void setBlobPath(Path path) {
            this.path = path;
        }

        public Path getBlobPath() {
            return this.path;
        }
    }

    void inserFromBytes(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "insert into blob_example values(:pk, :value)";
        var variables = TgBindVariables.of().addInt("pk").addBlob("value");
        var parameterMapping = TgParameterMapping.of(variables);

        try (var ps = tm.getSession().createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (int i = 0; i < 10; i++) {
                    var parameter = TgBindParameters.of() //
                            .addInt("pk", i) //
                            .addBlob("value", new byte[] { 1, 2, 3 }) // temporary file is created internally using IceaxeObjectFactory.getDefaultInstance()
                    ;
                    transaction.executeAndGetCountDetail(ps, parameter); // delete temporary file in executeAndGetCountDetail()
                }
                return;
            });
        }
    }

    void insertFromEntity(TsurugiTransactionManager tm, List<ExampleBlobEntity> entityList) throws IOException, InterruptedException {
        var sql = "insert into blob_example values(:pk, :value)";
        var parameterMapping = TgParameterMapping.of(ExampleBlobEntity.class) //
                .addInt("pk", ExampleBlobEntity::getPk) //
                .addBlob("value", ExampleBlobEntity::getBlobValue); // TgBlob getter

        try (var ps = tm.getSession().createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (var entity : entityList) {
                    transaction.executeAndGetCountDetail(ps, entity);
                }
            });
        }
    }

    void insertFromEntityPath(TsurugiTransactionManager tm, List<ExampleBlobEntity> entityList) throws IOException, InterruptedException {
        var sql = "insert into blob_example values(:pk, :value)";
        var parameterMapping = TgParameterMapping.of(ExampleBlobEntity.class) //
                .addInt("pk", ExampleBlobEntity::getPk) //
                .addBlobPath("value", ExampleBlobEntity::getBlobPath); // Path getter

        try (var ps = tm.getSession().createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                for (var entity : entityList) {
                    transaction.executeAndGetCountDetail(ps, entity);
                }
            });
        }
    }

    // select

    void select(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select pk, value from blob_example order by pk";
        var resultMapping = TgResultMapping.of(record -> record);

        try (var ps = tm.getSession().createQuery(sql, resultMapping)) {
            tm.execute(transaction -> {
                transaction.executeAndForEach(ps, record -> {
                    try (TgBlobReference blob = record.getBlob("value")) {
                        try (InputStream is = blob.openInputStream()) {
                            // read from `is`
                        }
                    } // close large object cache in blob.close()
                });
            });
        }
    }

    void select_copyTo(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select pk, value from blob_example order by pk";
        var resultMapping = TgResultMapping.of(record -> record);

        try (var ps = tm.getSession().createQuery(sql, resultMapping)) {
            tm.execute(transaction -> {
                transaction.executeAndForEach(ps, record -> {
                    try (TgBlobReference blob = record.getBlob("value")) {
                        blob.copyTo(Path.of("/path/to/destination"));
                    }
                });
            });
        }
    }

    void selectToResultEntity(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select pk, value from blob_example order by pk";

        try (var ps = tm.getSession().createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps); // copy to temporary file

            for (var entity : list) {
                try (TgBlob blob = entity.getBlob("value")) {
                    try (var is = blob.openInputStream()) {
                        // read from `is`
                    }
                } // delete temporary file in blob.close()
            }
        }
    }

    void selectToEntity(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select pk, value from blob_example order by pk";
        var resultMapping = TgResultMapping.of(ExampleBlobEntity::new) //
                .addInt("pk", ExampleBlobEntity::setPk) //
                .addBlob("value", ExampleBlobEntity::setBlobValue);

        try (var ps = tm.getSession().createQuery(sql, resultMapping)) {
            List<ExampleBlobEntity> list = tm.executeAndGetList(ps); // copy to temporary file

            for (var entity : list) {
                try (TgBlob blob = entity.getBlobValue()) {
                    try (var is = blob.openInputStream()) {
                        // read from `is`
                    }
                } // delete temporary file in blob.close()
            }
        }
    }

    void selectSingle(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select value from blob_example order by pk";
        var resultMapping = TgSingleResultMapping.ofBlob();

        try (var ps = tm.getSession().createQuery(sql, resultMapping)) {
            List<TgBlob> list = tm.executeAndGetList(ps); // copy to temporary file

            for (var blob : list) {
                try (blob) {
                    try (var is = blob.openInputStream()) {
                        // read from `is`
                    }
                } // delete temporary file in blob.close()
            }
        }
    }

    // docker run -d -p 12345:12345 --name tsurugi -v D:/tmp/client:/mnt/client -v D:/tmp/tsurugi:/opt/tsurugi/var/data/log ghcr.io/project-tsurugi/tsurugidb:latest
    void largeObjectPathMapping() throws IOException, InterruptedException {
        var connector = TsurugiConnector.of("tcp://localhost:12345");
        var sessionOption = TgSessionOption.of() //
                .addLargeObjectPathMappingOnSend(Path.of("D:/tmp/client"), "/mnt/client") //
                .addLargeObjectPathMappingOnReceive("/opt/tsurugi/var/data/log", Path.of("D:/tmp/tsurugi"));
        try (var session = connector.createSession(sessionOption)) {
            var tm = session.createTransactionManager(TgTxOption.ofOCC());

            { // insert
                var sql = "insert into blob_example values(:pk, :value)";
                var variables = TgBindVariables.of().addInt("pk").addBlob("value");
                var parameterMapping = TgParameterMapping.of(variables);
                try (var ps = session.createStatement(sql, parameterMapping)) {
                    tm.execute(transaction -> {
                        var blobFile = Path.of("D:/tmp/client/blob.bin");
                        Files.write(blobFile, new byte[] { 0x31, 0x32, 0x33 });
                        var parameter = TgBindParameters.of().addInt("pk", 10).addBlob("value", blobFile);
                        transaction.executeAndGetCountDetail(ps, parameter);
                    });
                }
            }
            { // select
                var sql = "select * from blob_example order by pk";
                var resultMapping = TgResultMapping.of(record -> record);
                try (var ps = session.createQuery(sql, resultMapping)) {
                    tm.executeAndForEach(ps, record -> {
                        try (var blob = record.getBlob("value")) {
                            byte[] buf = blob.readAllBytes();
                            System.out.println(Arrays.toString(buf));
                        }
                    });
                }
            }
        }
    }
}
