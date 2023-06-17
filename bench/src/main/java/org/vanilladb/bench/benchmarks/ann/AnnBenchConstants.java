package org.vanilladb.bench.benchmarks.ann;

import org.vanilladb.bench.util.BenchProperties;

public class AnnBenchConstants {

    public static final int NUM_ITEMS;
    public static final int NUM_DIMENSION;
    public static final int QUERY_SEED;
    public static final int DATASET_SEED;

    static {
        NUM_ITEMS = BenchProperties.getLoader().getPropertyAsInteger(
                AnnBenchConstants.class.getName() + ".NUM_ITEMS", 100000);
        NUM_DIMENSION = BenchProperties.getLoader().getPropertyAsInteger(
                AnnBenchConstants.class.getName() + ".NUM_DIMENSIONS", 8);
        DATASET_SEED = BenchProperties.getLoader().getPropertyAsInteger(
                AnnBenchConstants.class.getName() + ".DATASET_SEED", 1337);
        QUERY_SEED = BenchProperties.getLoader().getPropertyAsInteger(
                AnnBenchConstants.class.getName() + ".QUERY_SEED", 42);
                
    }
}
