package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.*;

public class EuclideanFn extends DistanceFn {
static final VectorSpecies<Double> SPECIES_d = DoubleVector.SPECIES_PREFERRED;
static final VectorSpecies<Float> SPECIES_f = FloatVector.SPECIES_PREFERRED;
    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
    	 double[] vec_d = vec.asJavaVal_d();
    	
         int i = 0;
         DoubleVector sum = DoubleVector.zero(SPECIES_d);
         for (; i < SPECIES_d.loopBound(vec.dimension()); i += SPECIES_d.length()) {
        	 DoubleVector v = DoubleVector.fromArray(SPECIES_d, vec_d, i);
 	         DoubleVector q = DoubleVector.fromArray(SPECIES_d, query_d, i);
 	         DoubleVector diff = v.sub(q);
 	         sum = diff.fma(diff, sum);
 	    }
        double sum_d = sum.reduceLanes(VectorOperators.ADD);
    
        
 	    // Dealing with tail of dim % SPECIES.length()
 	    for (; i < vec.dimension(); i++) {
 		   double diff = query.get(i) - vec.get(i);
           sum_d += diff * diff;
 	    }
        
        return Math.sqrt(sum_d);
    }
    @Override
    protected float calculateDistance2(VectorConstant vec) {
        float[] vec_f = vec.asJavaVal_f();

        int i = 0;
        FloatVector sum = FloatVector.zero(SPECIES_f);
        for (; i < SPECIES_f.loopBound(vec.dimension()); i += SPECIES_f.length()) {
            FloatVector v = FloatVector.fromArray(SPECIES_f, vec_f, i);
            FloatVector q = FloatVector.fromArray(SPECIES_f, query_f, i);
            FloatVector diff = v.sub(q);
            sum = diff.fma(diff, sum);
        }
        float sum_f = sum.reduceLanes(VectorOperators.ADD);


        // Dealing with tail of dim % SPECIES.length()
        for (; i < vec.dimension(); i++) {
            float diff = query.get(i) - vec.get(i);
            sum_f += diff * diff;
        }

        return sum_f;
    }

}