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
    private String tblName;
    private int numDimension, numItems, numNeighbors;

    // Hyper Parameters
    private static int numGroups = 3;
    private static Double tolerence = 0.0;

    // Utils
	private static boolean centerLoaded = false;
	private static int curItems = 0;
	private static VectorConstant[] groupCenter = new VectorConstant[numGroups];
	private static KNNHelper knnHelper;

	private String embField = "i_emb";
	private Random random = new Random();
    
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

	public List<VectorConstant> findKNN(VectorConstant query, Transaction tx) {
		DistanceFn distFn = new EuclideanFn("vector");
		distFn.setQueryVector(query);
		TablePlan p = new TablePlan(tblName, tx);

		// 0. Load groupCenter if it's unloaded
		loadCenters(tx);
		
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
			//TODO check id may be repeat
			id[i] = ridList.get(i).id();
		}
		
		// 3. Search top K vector in the group
        KSmallest(dist, id, 0, vecInGroup - 1, numNeighbors);

		// 4. Create List<VectorConstant> from top K idx list
		List<Integer> idxList = new ArrayList<Integer>();
		List<VectorConstant> knnVec = new ArrayList<VectorConstant>();
		for(int i=0; i<numNeighbors; i++)
            idxList.add(id[i]);
		Collections.sort(idxList);

		int idx_it = 0, scan_it = 0;
        s = (TableScan) p.open();
        s.beforeFirst();
        while (s.next()){
			//TODO check after beforeFirst rid always equal
			if(scan_it == idxList.get(idx_it)) {
				idx_it ++;
				knnVec.add((VectorConstant) s.getVal(embField));
			}
			scan_it ++;
        }
        s.close();

		return knnVec;
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
		//TODO check why not directly get vector
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
			//TODO check why not directly get vector
			VectorConstant vec = knnHelper.getVec(s.getRecordId(), tx);
			distFn.setQueryVector(vec);

			Double minDist = Double.MAX_VALUE;
			
			for(int i = 0; i < numGroups; i++){
				Double dist = distFn.distance(groupCenter[i]);
				if(dist < minDist){
					minDist = dist;
					//TODO check after beforeFirst rid always equal
					//TODO check could merge calculate center to here
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
			//TODO check why not directly get vector
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
			//TODO check why not directly get vector
			//TODO check after beforeFirst rid always equal
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
			//TODO check after beforeFirst rid always equal
			Constant gid = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(groupId[rid]));
			knnHelper.updateGroupId(s.getRecordId(), gid, tx);
			rid++;
        }
        s.close();

		for(int i=0; i<numGroups; i++) {
			Constant gid = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(i+1));
			knnHelper.updateGroupCenter(gid, groupCenter[i], tx);
		}
    }

	//TODO change to synchronize centerLoaded or lock
	private synchronized static void loadCenters(Transaction tx) {
		if(!centerLoaded) 
		{
			List<VectorConstant> centerList = knnHelper.queryGroupCenters(tx);
			for(int i=0; i<numGroups; i++) groupCenter[i] = centerList.get(i);
		}
		centerLoaded = true;
	}
	//TODO this best time complexity is O(N) worst time complexity is O(N^2)
	// But heap sort time complexity worst time complexity is O(NlogK) where K is fix to 20.
    private void KSmallest(Double[] arr, int[] idx, int l, int r, int k) {
		// Using quickselect algorithm

        int pivot = Partition(arr, idx, l, r);
  
        if (pivot < k - 1)
            KSmallest(arr, idx, pivot + 1, r, k);
  
        else if (pivot > k - 1)
            KSmallest(arr, idx, l, pivot - 1, k);

		// end if (pivot == k - 1)
    }
	private int Partition(Double[] arr, int[] idx, int l, int r) {
        Double pivot = arr[r];
		int partition = l;
        for (int i = l; i <= r; i++) {
            if (arr[i] < pivot) {
                Double tmpD = arr[i];
                arr[i] = arr[partition];
                arr[partition] = tmpD;

				int tmpI = idx[i];
				idx[i] = idx[partition];
				idx[partition] = tmpI;

                partition++;
            }
        }
		
		Double tmpD = arr[r];
		arr[r] = arr[partition];
		arr[partition] = tmpD;

		int tmpI = idx[r];
		idx[r] = idx[partition];
		idx[partition] = tmpI;
  
        return partition;
    }
}