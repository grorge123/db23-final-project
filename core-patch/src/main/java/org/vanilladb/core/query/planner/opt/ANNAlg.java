package org.vanilladb.core.query.planner.opt;

import java.util.Random;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.ByteHelper;

public class ANNAlg {
    private KNNIndex knn;

    // Bench Constant
    private int numDimension, numItems;

    // Hyper Parameters
    private int numGroups = 8;
    private int maxIters = 300;

    // KMeans Utils
    VectorConstant[] groupCenter = new VectorConstant[numGroups];
    int[] groupId = new int[100000];
    

    // dummy function, should be implemented elsewhere
    public VectorConstant getVec(int recordId) 
    {
        return new VectorConstant(numDimension);
    }

    public ANNAlg(String tblName, int _numDimension, int _numItems) {
        knn = new KNNIndex(tblName);
        numDimension = _numDimension;
        numItems = _numItems;
    }
   
    public void KMeans(Transaction tx){
        // 呼叫到numItems 次才會計算group ID
        // This function should be called once after inserting all vectors.

        // Initializing group centers
        Random r = new Random();
        for(int i=0; i<numGroups; i++)
            groupCenter[i] = getVec(r.nextInt(numItems)); 

        // Updating group centers
        for(int t=0; t<maxIters; t++)
        {
            int[] nearestCCs = new int[numItems];
            double[][] coordSum = new double[numGroups][numDimension];
            int[] pointNum = new int[numGroups];

            for (int i = 0; i < numItems; i++) 
            {
                int nearestCC = 0;
                double minDistance = Double.MAX_VALUE;
                for (int j = 0; j < numGroups; j++) 
                {
                    double distance = euclideanDistance(getVec(i), groupCenter[j]);
                    if (distance < minDistance) {
                        minDistance = distance;
                        nearestCC = j;
                    }
                }
                nearestCCs[i] = nearestCC;
                for (int k = 0; k < numDimension; k++) {
                    coordSum[nearestCC][k] += getVec(i).get(k);
                }
                pointNum[nearestCC]++;
            }

            double[][] updatedCCs = new double[numGroups][numDimension];
            for (int i = 0; i < numGroups; i++) {
                if (pointNum[i] != 0) {
                    for (int j = 0; j < numDimension; j++) {
                        updatedCCs[i][j] = coordSum[i][j] / pointNum[i];
                    }
                }
            }
        }
        
        /*
         nearest_ccs = np.ndarray(X.shape[0])
        coord_sum = np.zeros((self.n_clusters, X.shape[1]))
        point_num = np.zeros(self.n_clusters)

        for i in range(X.shape[0]):
          nearest_cc = np.argmin([np.linalg.norm(X[i]-cluster_center[j]) for j in range(self.n_clusters)])
          nearest_ccs[i] = nearest_cc
          coord_sum[nearest_cc]+=X[i]
          point_num[nearest_cc]+=1

        upd_ccs = np.ndarray((self.n_clusters, X.shape[1]))
        for i in range(self.n_clusters):
          if(point_num[i]!=0):
            upd_ccs[i] = coord_sum[i]/point_num[i]
        return upd_ccs
         */
    }
    private double euclideanDistance(VectorConstant v1, VectorConstant v2) {
        double sum = 0;
        for (int i = 0; i < v1.dimension(); i++) {
            double diff = v2.get(i) - v1.get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
}

        
// Algorithms

2000 萬萬
// 1. Given an initial set of k means m_1, ..., m_k
// 2. Assign each data point to the cluster with the nearest mean based on Euclidean distance
// 3. Recalculate means for data points assigned to each cluster
// 4. Repeat steps 2 and 3 until converged

/*def predict(self, X):
        # [TODO] For each x in X, predict the cluster that x belongs to.
        nearest_ccs = np.ndarray(X.shape[0])
        for i in range(X.shape[0]):
          nearest_ccs[i] = np.argmin([np.linalg.norm(X[i]-self.cluster_center[j]) for j in range(self.n_clusters)])
          # argmin: get idx of min
        return nearest_ccs
        
    def score(self, cluster_center, X):
        avg_dist = 0
        for xx in X:
            min_dist = np.min([np.linalg.norm(xx-cluster_center[i]) for i in range(self.n_clusters)])
            total_dist = np.sum([np.linalg.norm(xx-cluster_center[i]) for i in range(self.n_clusters)])
            avg_dist -= (2 * min_dist - total_dist)
        avg_dist /= X.shape[0]
        return avg_dist*/
    
    