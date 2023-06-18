package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.server.VanillaDb;

import java.util.List;

public class KNNNeighborPlan implements Plan {
    private Plan child;
    private Transaction tx;
    private DistanceFn distfn;

    public KNNNeighborPlan(Plan p, DistanceFn distFn, Transaction _tx) {
        this.distfn = distFn;
        this.child = new SortPlan(p, distFn, _tx);
        tx = _tx;
    }

    @Override
    public Scan open() {
        List<Constant> lvc = VanillaDb.knnAlg.findKNN(distfn.getQuery(), tx);
        return new KNNNeighborScan(lvc);
    }

    @Override
    public long blocksAccessed() {
        return child.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    @Override
    public Histogram histogram() {
        return child.histogram();
    }

    @Override
    public long recordsOutput() {
        return child.recordsOutput();
    }
}       
