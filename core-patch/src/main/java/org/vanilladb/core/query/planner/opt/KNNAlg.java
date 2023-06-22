package org.vanilladb.core.query.planner.opt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.List;
import java.util.Random;
import java.util.Set;

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
import org.vanilladb.core.util.CoreProperties;

import java.util.Arrays;
import java.util.Comparator;
import java.lang.Thread;
import java.util.stream.*;


public class KNNAlg{


	// Bench Setting
	private String tblName;
	private int numDimension, numItems, numNeighbors;

	// Hyper Parameters
	private static int numGroups = CoreProperties.getLoader().getPropertyAsInteger(KNNAlg.class.getName() + ".NUM_GROUPS", 9990); 
	private static int maxIter = 100;
	private static int groupMultiplier = CoreProperties.getLoader().getPropertyAsInteger(KNNAlg.class.getName() + ".GROUP_MULTIPLIER", 9990);

	// Utils
	private static boolean centerLoaded = false;
	private static int curItems = 0;
	private static VectorConstant[] groupCenter;
	// private static int[] groupSz;
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
		knnHelper = new KNNHelper(tblName, numDimension, numGroups);
		while (groupCenter == null){
			synchronized (initGroupCenter){
				if(groupCenter == null){
					groupCenter = new VectorConstant[numGroups];
					// groupSz = new int[numGroups];
					Transaction tx = VanillaDb.txMgr().newTransaction(
							Connection.TRANSACTION_SERIALIZABLE, false);
					loadCenters(tx);
					tx.commit();
				}
			}
		}
	}

	synchronized public void UpdateGroupId(Transaction tx){
		curItems ++;

		try {
			if(curItems == numItems){
				Thread.sleep(3000);
				KMeans(tx);
			}
		} catch(InterruptedException e) {
				e.printStackTrace();
		}
	}

	public List<Constant> findKNN(VectorConstant query, Transaction tx) {
		DistanceFn distFn = new EuclideanFn("vector");
		distFn.setQueryVector(query);

		// 1. Assign query vector to a group
		Pair<Float, Integer>[] arr = IntStream.range(0, numGroups)
				.parallel()
				.mapToObj(i -> {
					Float dist = distFn.distance2(groupCenter[i]);
					return new Pair<>(dist, i);
				})
				.toArray(Pair[]::new);

		Comparator<Pair<Float, Integer>> pairComparator = Comparator.comparing(Pair::getKey);
		Arrays.parallelSort(arr, pairComparator);

		// 2. Calculate distance between query and all other vectors
		List<RecordId> ridList = new ArrayList<>();
		int idx = 0;
		while (idx < groupMultiplier){
			if(idx >= arr.length)break;
			Pair<Float, Integer> gp = arr[idx++];
			Constant const_gid = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(gp.getValue()));
			List<RecordId> tmpList = knnHelper.queryRecord(const_gid, tx);
			ridList.addAll(tmpList);
		}

		// 3. Search top K vector in the group
		List<Constant> knnVec = KSmallest(ridList, distFn, tx);

		return knnVec;
	}

	private void KMeans(Transaction tx) {
		groupCenter = new VectorConstant[numGroups];
		// groupSz = new int[numGroups];
		TablePlan p = new TablePlan(tblName, tx);
		DistanceFn distFn = new EuclideanFn("vector");
		Float prev_error = Float.MAX_VALUE, error;
		int[] groupId = new int[numItems];
		System.err.println("Groupnum = " + numGroups);
		KMeans_plusplus_init(p, distFn, tx);
		int cnt = 0;
		while(true) {
			KMeans_update(p, groupId, distFn, tx);
			error = KMeans_calError(p, groupId, tx);
			if(cnt >= maxIter || prev_error - error <= 0) break; 
			cnt++;
			prev_error = error;
			System.out.println("error of " + cnt +": " + error/numItems);
		}

		KMeans_store(p, groupId, tx);
	}

	private void KMeans_plusplus_init(TablePlan p, DistanceFn distFn, Transaction tx) {
		// Initializing group center from vectors
		TableScan s = (TableScan) p.open();
		s.beforeFirst();
		s.next();
		groupCenter[0] = (VectorConstant) s.getVal(embField);
		s.close();

		// Randomly choice next center based on dist to current center
		Float[] distList = new Float[numItems];
		VectorConstant currentCenter = groupCenter[0];

		for(int i=0; i<numItems; i++) distList[i] = Float.MAX_VALUE;

		for(int i=1; i<numGroups; i++)
		{
			distFn.setQueryVector(currentCenter);
			Float distSum = 0.f;
			// Calculating dist to nearest center for each vector
			s = (TableScan) p.open();
			s.beforeFirst();
			int rid = 0;
			while (s.next()){
				VectorConstant vec = (VectorConstant) s.getVal(embField);
				Float minDist = distList[rid];
				Float dist = distFn.distance2(vec);
				if(dist < minDist) minDist = dist;

				distList[rid] = minDist;
				distSum += minDist;
				rid ++;
			}	
			s.close();
			// Sampling vector based on its distance to nearest neighbor
			int sampleId = 0;
			Float randomValue = random.nextFloat() * (distSum - distList[0]) + distList[0];

			// The first group center is vector 0.
			// So we can start select from the second one.
			for(int j=1; j<numItems; j++) { 
				distList[j] += distList[j-1];
				if(distList[j] > randomValue) {
					sampleId = j;	
					break;
				}
			}
			// System.out.println("Center " + i + "is set as vector " + sampleId);
			
			s = (TableScan) p.open();
			s.beforeFirst();
			while (s.next()) {
				if (sampleId == 0)  break;
				sampleId--;
			}
			// Update next groupCenter and set it as current center
			currentCenter = (VectorConstant) s.getVal(embField);
			s.close();
           
			groupCenter[i] = currentCenter;
		}
	}

	private void KMeans_init(TablePlan p, Transaction tx) {
		// Initializing group center from vectors

		List<Integer> idxList = new ArrayList<Integer>();
		Set<Integer> checkDistinct = new HashSet<>();
		while(checkDistinct.size() < numGroups) {
//			checkDistinct.add(random.nextInt(numItems));
			checkDistinct.add(checkDistinct.size());
		}
        for(Integer it : checkDistinct){
			idxList.add(it);
		}
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

			Float minDist = Float.MAX_VALUE;

			for(int i = 0; i < numGroups; i++){
				Float dist = distFn.distance2(groupCenter[i]);
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
			if(memberCnt[i] !=  0){
				groupCenter[i] = coordSum[i].div(memberCnt[i]);
				// groupSz[i] = memberCnt[i];
			}
		}
	}

	private Float KMeans_calError(TablePlan p, int[] groupId, Transaction tx) {
		Float error = 0.f;
		TableScan s = (TableScan) p.open();
		s.beforeFirst();
		int rid = 0;
		while (s.next()){
			VectorConstant vec = (VectorConstant) s.getVal(embField);
			VectorConstant center = groupCenter[groupId[rid]];
			Float sum = 0.f;
			for (int i = 0; i < vec.dimension(); i++) {
				Float diff = (float) center.get(i) - vec.get(i);
				sum += diff * diff;
			}
			error += (float) Math.sqrt((double)sum);
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
			Constant gid = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(i));
			knnHelper.updateGroupCenter(gid, /*groupSz[i], */groupCenter[i], tx);
		}
	}

	//TODO change to synchronize centerLoaded or lock
	private synchronized static void loadCenters(Transaction tx) {
		if(!centerLoaded)
		{
			List<VectorConstant> centerList = knnHelper.queryGroupCenters(tx);
			// List<Constant> centerSzList = knnHelper.queryGroupSz(tx);
			if(centerList.size() == 0)return;
			for(int i=0; i<numGroups; i++) groupCenter[i] = centerList.get(i);
			// for(int i=0; i<numGroups; i++) groupSz[i] = (int)centerSzList.get(i).asJavaVal();
		}
		centerLoaded = true;
	}

	private List<Constant> KSmallest(List<RecordId> ridList, DistanceFn dfn, Transaction tx) {
		PriorityQueue<Pair<Float, Constant>> minHeap =
				new PriorityQueue<Pair<Float, Constant>>(numNeighbors, (a, b) -> (b.getKey() < a.getKey() ? -1 : 1));
		for (RecordId rid : ridList) {
			org.vanilladb.core.query.planner.opt.Pair<VectorConstant, Constant> tmp = knnHelper.getVecAndId(rid, tx);
			Pair<Float, Constant> num = new Pair<>(dfn.distance2(tmp.getKey()), tmp.getValue());
			minHeap.offer(num);
			if (minHeap.size() > numNeighbors) {
				minHeap.poll();
			}
		}

		List<Constant> res = new ArrayList<>();
		for (int i = 0; i < numNeighbors; i++) {
			// TODO need to handle group size < k
			if(minHeap.size() == 0){
				res.add(Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(2)));
				System.out.println("ERROR: Not found in group!");
			}else{
				Pair<Float, Constant> id = minHeap.poll();
				res.add(id.getValue());
			}
		}
		return res;
	}
}