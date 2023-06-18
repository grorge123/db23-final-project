package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public abstract class DistanceFn {

    protected VectorConstant query;
    protected double[] query_d;
    private String fieldName;

    public DistanceFn(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setQueryVector(VectorConstant query) {
        this.query = query;
        this.query_d = query.asJavaVal_d();
    }
    
    public double distance(VectorConstant vec) {
        // check vector dimension
        if (query.dimension() != vec.dimension()) {
            throw new IllegalArgumentException("Vector length does not match");
        }
        return calculateDistance(vec);
    }

    protected abstract double calculateDistance(VectorConstant vec);

    public VectorConstant getQuery() {
        return query;
    }

    public String fieldName() {
        return fieldName;
    }
}
