package org.vanilladb.core.query.planner.opt;

import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.planner.index.IndexSelector;
import org.vanilladb.core.query.planner.index.IndexUpdatePlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.IndexType;
import java.sql.Connection;
import java.util.Arrays;

public class KNNIndex {
    static boolean isInit = false;
    static String tbl;
    public KNNIndex(String _tbl){
        if(!isInit){
            tbl = _tbl;
            init();
        }
    }
    public void init(){
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        IndexUpdatePlanner iup = new IndexUpdatePlanner();
        String indexTbl = tbl + "index";
        Schema sch = new Schema();
        sch.addField("groupid", Type.INTEGER);
        sch.addField("recordid", Type.INTEGER);
        CreateTableData ctd = new CreateTableData(indexTbl, sch);
        iup.executeCreateTable(ctd, tx);
        CreateIndexData cid = new CreateIndexData("groupindex", tbl, Arrays.asList("groupid"), IndexType.HASH);
        iup.executeCreateIndex(cid, tx);
        tx.commit();
        isInit = true;
    }
}
