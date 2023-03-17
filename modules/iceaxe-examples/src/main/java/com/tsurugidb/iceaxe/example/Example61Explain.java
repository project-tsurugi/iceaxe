package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * explain example
 */
@SuppressWarnings("unused")
public class Example61Explain {

    void main() throws Exception {
        try (var session = Example02Session.createSession()) {
            explainSelect(session);
            explainSelectParameter(session);
            explainStatement(session);
            explainStatementParameter(session);
        }
    }

    void explainSelect(TsurugiSession session) throws IOException, PlanGraphException {
        var sql = "select * from TEST";

        try (var ps = session.createQuery(sql)) {
            var statementMetadata = ps.explain();
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }

    void explainSelectParameter(TsurugiSession session) throws IOException, PlanGraphException {
        var foo = TgBindVariable.ofInt("foo");
        var sql = "select * from TEST where foo=" + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        try (var ps = session.createQuery(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(foo.bind(123));
            var statementMetadata = ps.explain(parameter);
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }

    void explainStatement(TsurugiSession session) throws IOException, PlanGraphException {
        var sql = "update TEST set bar=123";

        try (var ps = session.createStatement(sql)) {
            var statementMetadata = ps.explain();
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }

    void explainStatementParameter(TsurugiSession session) throws IOException, PlanGraphException {
        var bar = TgBindVariable.ofLong("bar");
        var sql = "update TEST set bar=" + bar;
        var parameterMapping = TgParameterMapping.of(bar);

        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(bar.bind(123));
            var statementMetadata = ps.explain(parameter);
            var planGraph = statementMetadata.getLowPlanGraph();
        }
    }
}
