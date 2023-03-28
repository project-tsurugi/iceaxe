package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * explain example
 */
@SuppressWarnings("unused")
public class Example61Explain {

    void main() throws Exception {
        try (var session = Example02Session.createSession()) {
            explainQuery(session);
            explainPreparedQuery(session);
            explainStatement(session);
            explainPreparedStatement(session);
        }
    }

    void explainQuery(TsurugiSession session) throws IOException, PlanGraphException {
        var sql = "select * from TEST";

        try (var ps = session.createQuery(sql)) {
            var statementMetadata = ps.explain();
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }

    void explainPreparedQuery(TsurugiSession session) throws IOException, PlanGraphException {
        var foo = TgBindVariable.ofInt("foo");
        var sql = "select * from TEST where FOO=" + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        try (var ps = session.createQuery(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(foo.bind(123));
            var statementMetadata = ps.explain(parameter);
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }

    void explainStatement(TsurugiSession session) throws IOException, PlanGraphException {
        var sql = "update TEST set BAR=123";

        try (var ps = session.createStatement(sql)) {
            var statementMetadata = ps.explain();
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }

    void explainPreparedStatement(TsurugiSession session) throws IOException, PlanGraphException {
        var bar = TgBindVariable.ofLong("bar");
        var sql = "update TEST set BAR=" + bar;
        var parameterMapping = TgParameterMapping.of(bar);

        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(bar.bind(123));
            var statementMetadata = ps.explain(parameter);
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }
}
