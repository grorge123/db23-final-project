package org.vanilladb.core.query.planner.opt;

import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.planner.index.IndexSelector;
import org.vanilladb.core.query.planner.index.IndexUpdatePlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.predicate.*;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.index.IndexType;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.vanilladb.core.sql.predicate.Term.OP_EQ;

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
        cid = new CreateIndexData("recordindex", tbl, Arrays.asList("recordid"), IndexType.HASH);
        iup.executeCreateIndex(cid, tx);
        tx.commit();
        isInit = true;
    }
    public void update(int recordId, int groupId, Transaction tx){
        IndexUpdatePlanner iup = new IndexUpdatePlanner();
        Map<String, Expression> map = new HashMap<String, Expression>();
        map.put("groupid", new ConstantExpression(Constant.newInstance(Type.INTEGER, ByteBuffer.allocate(4).putInt(groupId).array())));
        Predicate pred = new Predicate(new Term(new FieldNameExpression("recordid"), OP_EQ, new ConstantExpression(Constant.newInstance(Type.INTEGER, ByteBuffer.allocate(4).putInt(recordId).array()))));
        ModifyData md = new ModifyData(tbl, map, pred);
        int updateCount = iup.executeModify(md, tx);
        if(updateCount == 0){
            List<String> fields = Arrays.asList("groupid", "recordid");
            List<Constant> vals = Arrays.asList(
                    Constant.newInstance(Type.INTEGER, ByteBuffer.allocate(4).putInt(groupId).array()),
                    Constant.newInstance(Type.INTEGER, ByteBuffer.allocate(4).putInt(recordId).array())
            );
            InsertData ind = new InsertData(tbl, fields, vals);
            iup.executeInsert(ind, tx);
        }
    }
    public void query(){

    }

}
