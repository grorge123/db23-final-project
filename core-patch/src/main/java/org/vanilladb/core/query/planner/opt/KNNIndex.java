package org.vanilladb.core.query.planner.opt;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.planner.Verifier;
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
import java.util.*;

import static org.vanilladb.core.sql.predicate.Term.OP_EQ;

public class KNNIndex {
    boolean isInit = false;
    String tbl;
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
        Schema sch = new Schema();
        sch.addField("groupid", Type.INTEGER);
        sch.addField("recordid", Type.INTEGER);
        CreateTableData ctd = new CreateTableData(tbl, sch);
        Verifier.verifyCreateTableData(ctd, tx);
        iup.executeCreateTable(ctd, tx);
        CreateIndexData cid = new CreateIndexData("groupindex", tbl, Arrays.asList("groupid"), IndexType.BTREE);
        Verifier.verifyCreateIndexData(cid, tx);
        iup.executeCreateIndex(cid, tx);
        cid = new CreateIndexData("recordindex", tbl, Arrays.asList("recordid"), IndexType.BTREE);
        Verifier.verifyCreateIndexData(cid, tx);
        iup.executeCreateIndex(cid, tx);
        tx.commit();
        isInit = true;
    }
    public void update(Constant recordId, Constant groupId, Transaction tx){
        IndexUpdatePlanner iup = new IndexUpdatePlanner();
        Map<String, Expression> map = new HashMap<String, Expression>();
        map.put("groupid", new ConstantExpression(groupId));
        Predicate pred = new Predicate(new Term(new FieldNameExpression("recordid"), OP_EQ, new ConstantExpression(recordId)));
        ModifyData md = new ModifyData(tbl, map, pred);
        int updateCount = iup.executeModify(md, tx);
        if(updateCount == 0){
            List<String> fields = Arrays.asList("groupid", "recordid");
            List<Constant> vals = Arrays.asList(groupId, recordId);
            InsertData ind = new InsertData(tbl, fields, vals);
            iup.executeInsert(ind, tx);
        }
    }
    public Constant queryGroup(Constant recordId, Transaction tx){
        TablePlan tp = new TablePlan(tbl, tx);
        Predicate pred = new Predicate(new Term(new FieldNameExpression("recordid"), OP_EQ, new ConstantExpression(recordId)));
        Plan selectPlan = IndexSelector.selectByBestMatchedIndex(tbl, tp, pred, tx);
        if (selectPlan == null)
            selectPlan = new SelectPlan(tp, pred);
        else
            selectPlan = new SelectPlan(selectPlan, pred);
        Scan s = selectPlan.open();
        s.beforeFirst();
        if(s.next()){
            Constant val = s.getVal("groupid");
            s.close();
            return val;
        }
        s.close();
        return  Constant.defaultInstance(Type.INTEGER);
    }

    public List<Constant> queryRecord(Constant groupId, Transaction tx){
        TablePlan tp = new TablePlan(tbl, tx);
        Predicate pred = new Predicate(new Term(new FieldNameExpression("groupid"), OP_EQ, new ConstantExpression(groupId)));
        Plan selectPlan = IndexSelector.selectByBestMatchedIndex(tbl, tp, pred, tx);
        if (selectPlan == null)
            selectPlan = new SelectPlan(tp, pred);
        else
            selectPlan = new SelectPlan(selectPlan, pred);
        Scan s = selectPlan.open();
        s.beforeFirst();
        List<Constant> reList = new ArrayList<Constant>();
        while (s.next()){
            reList.add(s.getVal("recordid"));
        }
        s.close();
        return  reList;
    }

}
