package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.*;

public class EuclideanFn extends DistanceFn {
static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
    	 double[] vec_d = vec.asJavaVal_d();
    	
         int i = 0;
         DoubleVector sum = DoubleVector.zero(SPECIES);
         for (; i < SPECIES.loopBound(vec.dimension()); i += SPECIES.length()) {
        	 DoubleVector v = DoubleVector.fromArray(SPECIES, vec_d, i);
 	         DoubleVector q = DoubleVector.fromArray(SPECIES, query_d, i);
 	         DoubleVector diff = v.sub(q);
 	         sum = diff.fma(diff, sum);
 	    }
        double sum_d = sum.reduceLanes(VectorOperators.ADD);
    
        
 	    // Dealing with tail of dim % SPECIES.length()
    	sum_d = 0;
 	    for (i=0 ; i < vec.dimension(); i++) {
 		   double diff = query.get(i) - vec.get(i);
           sum_d += diff * diff;
 	    }
        
        return Math.sqrt(sum_d);
    }

}