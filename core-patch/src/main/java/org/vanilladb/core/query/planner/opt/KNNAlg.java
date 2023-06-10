package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.ByteHelper;

public class KNNAlg{
    // Bench Setting
    String tblName;
    private int numDimension, numItems, numNeighbors;

    // Hyper Parameters
    private int numGroups = 3;
    private Double tolerence = 0.0;

    // Utils
	private KNNHelper knnHelper;
	private static int curItems = 0;
	private Random random = new Random();
    VectorConstant[] groupCenter = new VectorConstant[numGroups];
    
    public KNNAlg(String _tblName, int _numDimension, int _numItems, int _numNeighbors) {
        tblName = _tblName;
        numDimension = _numDimension;
        numItems = _numItems;
		numNeighbors = _numNeighbors;
        knnHelper = new KNNHelper(tblName, numDimension);
    }
   
    synchronized public void UpdateGroupId(Transaction tx){
        curItems ++;
        if(curItems == numItems) KMeans(tx);
    }

	public void findKNN(VectorConstant query, Transaction tx) {
		DistanceFn distFn = new EuclideanFn("vector");
		distFn.setQueryVector(query);
		TablePlan p = new TablePlan(tblName, tx);

		// TODO: load groupCenter for once (ONCE is especially important for thread safe)

		// 1. Assign query vector to a group
		int gid = 0;
		Double minDist = Double.MAX_VALUE;

		TableScan s = (TableScan) p.open();
		s.beforeFirst();
		for(int i = 0; i < numGroups; i++){
			Double dist = distFn.distance(groupCenter[i]);
			if(dist < minDist){
				minDist = dist;
				gid = i;
			}
		}
		s.close();
		
		// 2. Calculate distance between query and all other vectors
		Constant const_gid = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(gid));
		List<RecordId> ridList = knnHelper.queryRecord(const_gid, tx);
		int vecInGroup = ridList.size();

		Double[] dist = new Double[vecInGroup];
		int[] id = new int[vecInGroup];

		for(int i=0; i<vecInGroup; i++)
		{
			VectorConstant vec = knnHelper.getVec(ridList.get(i), tx);
			dist[i] = distFn.distance(vec);
			id[i] = ridList.get(i).id();
		}
		
		// 3. Search top K vector in the group
        KSmallest(dist, 0, vecInGroup - 1, numNeighbors); // function undone (but almost done)
		// return List<VectorConstant>
	}

    private void KMeans(Transaction tx) {
        TablePlan p = new TablePlan(tblName, tx);
		DistanceFn distFn = new EuclideanFn("vector");
		Double prev_error = Double.MAX_VALUE, error;
		int[] groupId = new int[numItems];

		KMeans_init(p, tx);

        while(true) {
			KMeans_update(p, groupId, distFn, tx);
            error = KMeans_calError(p, groupId, tx);
            if(error - prev_error <= tolerence) break;
            prev_error = error;
		}

		KMeans_store(p, groupId, tx);
    }

    private void KMeans_init(TablePlan p, Transaction tx) {
        // Initializing group center from vectors 
        // Further Optim: using K-Means++ method. Reference to calculateWeighedCentroid().

		List<Integer> idxList = new ArrayList<Integer>();
		List<RecordId> ridList = new ArrayList<RecordId>();
       
		for(int i=0; i<numGroups; i++)
            idxList.add(random.nextInt(numItems)+1);
		Collections.sort(idxList);

		int idx_it = 0, scan_it = 0;
        TableScan s = (TableScan) p.open();
        s.beforeFirst();
        while (s.next()){
			if(scan_it == idxList.get(idx_it)) {
				idx_it ++;
				ridList.add(s.getRecordId());
			}
			scan_it ++;
        }
        s.close();

        for(int i=0; i<numGroups; i++)
            groupCenter[i] = knnHelper.getVec(ridList.get(i), tx);
    }

	private void KMeans_update(TablePlan p, int[] groupId, DistanceFn distFn, Transaction tx) {
		// 1. Recluster by finding the nearest group center for each vector
		TableScan s = (TableScan) p.open();
		s.beforeFirst();
		int rid = 0;
		while (s.next()){
			VectorConstant vec = knnHelper.getVec(s.getRecordId(), tx);
			distFn.setQueryVector(vec);

			Double minDist = Double.MAX_VALUE;
			
			for(int i = 0; i < numGroups; i++){
				Double dist = distFn.distance(groupCenter[i]);
				if(dist < minDist){
					minDist = dist;
					groupId[rid] = i;
				}
			}
			rid ++;
		}
		s.close();

		// 2. Recompute groupCenter according to new clustering result
		VectorConstant[] coordSum = new VectorConstant[numGroups];
		int[] memberCnt = new int[numGroups];
		for(int i=0; i<numGroups; i++) {
			memberCnt[i] = 0;
			coordSum[i] = VectorConstant.zeros(numDimension);
		}

		s = (TableScan) p.open();
		s.beforeFirst();
		rid = 0;
		while (s.next()){
			VectorConstant vec = knnHelper.getVec(s.getRecordId(), tx);
			coordSum[groupId[rid]].add(vec);
			memberCnt[groupId[rid]] ++;
			rid++;
		}
		s.close();
		
		for(int i=0; i<numGroups; i++) {
			if(memberCnt[i] !=  0)
				groupCenter[i] = coordSum[i].div(memberCnt[i]);
		}
	}

	private Double KMeans_calError(TablePlan p, int[] groupId, Transaction tx) {
		Double error = 0.0;
		TableScan s = (TableScan) p.open();
		s.beforeFirst();
		int rid = 0;
		while (s.next()){
			VectorConstant vec = knnHelper.getVec(s.getRecordId(), tx);
			VectorConstant center = groupCenter[groupId[rid]];
			double sum = 0;
			for (int i = 0; i < vec.dimension(); i++) {
				double diff = center.get(i) - vec.get(i);
				sum += diff * diff;
			}
			error += Math.sqrt(sum);
			rid++;
		}
		s.close();
		return error;
	}

	private void KMeans_store(TablePlan p, int[] groupId, Transaction tx) {
		// Storing groupId and groupCenter back to tables.

        TableScan s = (TableScan) p.open();
        s.beforeFirst();
		int rid = 0;
        while (s.next()){
			Constant gid = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(groupId[rid]));
			knnHelper.update(s.getRecordId(), gid, tx);
			rid++;
        }
        s.close();

		// TODO: storing groupCenter back
    }

	
    private void KSmallest(Double[] arr, int low, int high, int k) {
		// Using quickselect algorithm
        int pivot = Partition(arr, low, high);
  
        // if (pivot == k - 1), end.
  
        if (pivot < k - 1)
            KSmallest(arr, pivot + 1, high, k);
  
        else if (pivot > k - 1)
            KSmallest(arr, low, pivot - 1, k);
    }

	private int Partition(Double[] arr, int low, int high) {
        // Move smaller elements to the left of pivot
        // and higher elements to the right
    
        Double pivot = arr[high];
		int pivotloc = low;
        for (int i = low; i <= high; i++) {
            if (arr[i] < pivot) {
                Double temp = arr[i];
                arr[i] = arr[pivotloc];
                arr[pivotloc] = temp;
                pivotloc++;
            }
        }
  
        Double temp = arr[high];
        arr[high] = arr[pivotloc];
        arr[pivotloc] = temp;
  
        return pivotloc;
    }
}