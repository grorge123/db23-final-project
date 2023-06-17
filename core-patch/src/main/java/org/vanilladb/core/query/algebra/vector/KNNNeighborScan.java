package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;

import java.util.List;

public class KNNNeighborScan implements Scan {

    Scan s;
    List<Constant> vector_list;
    int cur;

    public KNNNeighborScan(List<Constant> lck, Scan s) {
        this.s = s;
        vector_list = lck;
        cur = -1;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
        cur = -1;
    }

    @Override
    public boolean next() {
        cur++;
        return !(cur == vector_list.size());
    }

    @Override
    public void close() {
        s.close();
    }

    @Override
    public boolean hasField(String fldName) {
        return s.hasField(fldName);
    }

    @Override
    public Constant getVal(String fldName) {
        return vector_list.get(cur);
    }
}
