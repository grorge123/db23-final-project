package org.vanilladb.core.query.planner.opt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.List;
import java.util.Random;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.ByteHelper;
import java.util.AbstractMap;
public class KNNAlg{

	// define pair structure
	public class Pair<K, V> extends AbstractMap.SimpleEntry<K, V> {
		public Pair(K key, V value) {
			super(key, value);
		}
	}
	// Bench Setting
	private String tblName;
	private int numDimension, numItems, numNeighbors;

	// Hyper Parameters
	private static int numGroups = 3;
	private static Double tolerence = 0.0;

	// Utils
	private static boolean centerLoaded = false;
	private static int curItems = 0;
	private static VectorConstant[] groupCenter;
	private static Object initGroupCenter = new Object();
	private static KNNHelper knnHelper;

	private String embField = "i_emb";
	private static Random random;
	static{
		long seed = 100;
		random = new Random();
        random.setSeed(seed);
    }

	public KNNAlg(String _tblName, int _numDimension, int _numItems, int _numNeighbors) {
		tblName = _tblName;
		numDimension = _numDimension;
		numItems = _numItems;
		numNeighbors = _numNeighbors;
		knnHelper = new KNNHelper(tblName, numDimension);
		while (groupCenter == null){
			synchronized (initGroupCenter){
				if(groupCenter == null){
					groupCenter = new VectorConstant[numGroups];
					Transaction tx = VanillaDb.txMgr().newTransaction(
							Connection.TRANSACTION_SERIALIZABLE, false);
					loadCenters(tx);
					tx.commit();
				}
			}
		}
	}

	synchronized public void UpdateGroupId(Transaction tx){
		//TODO last tx not commit but call KMeans
		curItems ++;
		if(curItems == numItems)
		{
			KMeans(tx);
		}
	}

	public List<Constant> findKNN(VectorConstant query, Transaction tx) {
		//TODO erase print time
		DistanceFn distFn = new EuclideanFn("vector");
		distFn.setQueryVector(query);
		TablePlan p = new TablePlan(tblName, tx);

		// 0. Load groupCenter if it's unloaded

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

		// 3. Search top K vector in the group
		List<Constant> knnVec = KSmallest(ridList, distFn, tx);

		return knnVec;
	}

	private void KMeans(Transaction tx) {
		groupCenter = new VectorConstant[numGroups];
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

		for(int i=0; i<numGroups; i++)
			idxList.add(random.nextInt(numItems)+1);
		Collections.sort(idxList);

		int idx_it = 0, scan_it = 0;
		TableScan s = (TableScan) p.open();
		s.beforeFirst();
		while (s.next()){
			if(scan_it == idxList.get(idx_it)) {
				groupCenter[idx_it] = (VectorConstant) s.getVal(embField);
				idx_it ++;
			}
			if(idx_it == numGroups) break;
			scan_it ++;

		}
		s.close();
	}

	private void KMeans_update(TablePlan p, int[] groupId, DistanceFn distFn, Transaction tx) {
		// 1. Recluster by finding the nearest group center for each vector
		TableScan s = (TableScan) p.open();
		s.beforeFirst();
		int rid = 0;
		while (s.next()){
			VectorConstant vec = (VectorConstant) s.getVal(embField);
			distFn.setQueryVector(vec);

			Double minDist = Double.MAX_VALUE;

			for(int i = 0; i < numGroups; i++){
				Double dist = distFn.distance(groupCenter[i]);
				if(dist < minDist){
					minDist = dist;
					//TODO check could merge calculate center to here // further optim
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
			VectorConstant vec = (VectorConstant) s.getVal(embField);
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
			VectorConstant vec = (VectorConstant) s.getVal(embField);
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
			if(centerList.size() == 0)return;
			for(int i=0; i<numGroups; i++) groupCenter[i] = centerList.get(i);
		}
		centerLoaded = true;
	}

	private List<Constant> KSmallest(List<RecordId> ridList, DistanceFn dfn, Transaction tx) {
		PriorityQueue<Pair<Double, Constant>> maxHeap =
				new PriorityQueue<Pair<Double, Constant>>(numNeighbors, (a, b) -> (b.getKey() > a.getKey() ? 1 : -1));
		for (RecordId rid : ridList) {
			org.vanilladb.core.query.planner.opt.Pair<VectorConstant, Constant> tmp = knnHelper.getVecAndId(rid, tx);
			Pair<Double, Constant> num = new Pair<>(dfn.distance(tmp.getKey()), tmp.getValue());
			maxHeap.offer(num);
			if (maxHeap.size() > numNeighbors) {
				maxHeap.poll();
			}
		}

		List<Constant> res = new ArrayList<>();
		for (int i = 0; i < numNeighbors; i++) {
			// TODO need to handle group size < k
			if(maxHeap.size() == 0){
				res.add(Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(2)));
			}else{
				res.add(maxHeap.poll().getValue());
			}
		}
		return res;
	}
}