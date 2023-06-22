package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public abstract class DistanceFn {

    protected VectorConstant query;
    protected float[] query_f;
    protected double[] query_d;
    private String fieldName;

    public DistanceFn(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setQueryVector(VectorConstant query) {
        this.query = query;
        this.query_d = query.asJavaVal_d();
        this.query_f = query.asJavaVal_f();
    }
    
    public double distance(VectorConstant vec) {
        // check vector dimension
        if (query.dimension() != vec.dimension()) {
            throw new IllegalArgumentException("Vector length does not match");
        }
        return calculateDistance(vec);
    }

    public float distance2(VectorConstant vec) {
        // check vector dimension
        if (query.dimension() != vec.dimension()) {
            throw new IllegalArgumentException("Vector length does not match");
        }
        return calculateDistance2(vec);
    }

    protected abstract double calculateDistance(VectorConstant vec);
    protected abstract float calculateDistance2(VectorConstant vec);

    public VectorConstant getQuery() {
        return query;
    }

    public String fieldName() {
        return fieldName;
    }
}
