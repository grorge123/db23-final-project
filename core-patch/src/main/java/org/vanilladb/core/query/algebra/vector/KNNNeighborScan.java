package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;

import java.util.List;

public class KNNNeighborScan implements Scan {
    List<Constant> vector_list;
    int cur;

    public KNNNeighborScan(List<Constant> lck) {
        vector_list = lck;
        cur = -1;
    }

    @Override
    public void beforeFirst() {
        cur = -1;
    }

    @Override
    public boolean next() {
        cur++;
        return !(cur == vector_list.size());
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasField(String fldName) {
        return fldName == "i_id";
    }

    @Override
    public Constant getVal(String fldName) {
        return vector_list.get(cur);
    }
}
