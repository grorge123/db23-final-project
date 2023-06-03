/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.server;

import java.sql.Connection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.query.planner.opt.KNNIndex;
import org.vanilladb.core.remote.jdbc.JdbcStartUp;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.ByteHelper;

public class StartUp {
	private static Logger logger = Logger.getLogger(StartUp.class.getName());

	public static void main(String args[]) throws Exception {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");

		// configure and initialize the database
		VanillaDb.init(args[0]);

		// start up the listening port
		JdbcStartUp.startUp(1099);

		if (logger.isLoggable(Level.INFO))
			logger.info("database server ready");
		// For test KNN Index
//		System.out.println("TEST");
//		KNNIndex knn = new KNNIndex("itemindex");
//		Transaction tx = VanillaDb.txMgr().newTransaction(
//				Connection.TRANSACTION_SERIALIZABLE, false);
//		Constant reId1 = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(40));
//		Constant reId2 = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(52));
//		Constant grId1 = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(6));
//		Constant grId2 = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(6));
//		knn.update(reId1, grId1, tx);
//		knn.update(reId2, grId2, tx);
//		Constant regrId1 = knn.queryGroup(reId1, tx);
//		List<Constant> listId1 = knn.queryRecord(grId1, tx);
//		System.out.println("recordId1:");
//		System.out.println(regrId1.toString());
//		System.out.println("listId1:");
//		for(Constant k : listId1){
//			System.out.println(k.toString());
//		}
//		tx.commit();
//		System.out.println("TEST FINISH");
	}
}
