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
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.predicate.*;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.util.ByteHelper;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.*;

import static org.vanilladb.core.sql.predicate.Term.OP_EQ;

public class KNNIndex {
    boolean isInit = false;
    String tbl;
    String originTble;
    TableInfo ti;
    String embField = "i_emb";

    // The table name which searched by KNN
    public KNNIndex(String _tbl){
        if(!isInit){
            originTble = _tbl;
            tbl = originTble + "indextable";
            init();
        }
    }
    public void init(){
        Transaction tx = VanillaDb.txMgr().newTransaction(
                Connection.TRANSACTION_SERIALIZABLE, false);
        ti = VanillaDb.catalogMgr().getTableInfo(originTble, tx);
        IndexUpdatePlanner iup = new IndexUpdatePlanner();
        Schema sch = new Schema();
        sch.addField("groupid", Type.INTEGER);
        sch.addField("recordid", Type.INTEGER);
        sch.addField("blockid", Type.INTEGER);
        sch.addField("filename", Type.VARCHAR);
        CreateTableData ctd = new CreateTableData(tbl, sch);
        Verifier.verifyCreateTableData(ctd, tx);
        iup.executeCreateTable(ctd, tx);
        CreateIndexData cid = new CreateIndexData("groupindex", tbl, Arrays.asList("groupid"), IndexType.BTREE);
        Verifier.verifyCreateIndexData(cid, tx);
        iup.executeCreateIndex(cid, tx);
        cid = new CreateIndexData("recordindex", tbl, Arrays.asList("recordid", "blockid", "filename"), IndexType.BTREE);
        Verifier.verifyCreateIndexData(cid, tx);
        iup.executeCreateIndex(cid, tx);
        tx.commit();
        isInit = true;
    }
    public void update(RecordId recordId, Constant groupId, Transaction tx){
        Constant recordIdId = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(recordId.id()));
        Constant blockId = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(recordId.block().number()));
        Constant fileName = Constant.newInstance(Type.INTEGER, recordId.block().fileName().getBytes());
        IndexUpdatePlanner iup = new IndexUpdatePlanner();
        Map<String, Expression> map = new HashMap<String, Expression>();
        map.put("groupid", new ConstantExpression(groupId));
        Predicate pred = new Predicate(new Term(new FieldNameExpression("recordid"), OP_EQ, new ConstantExpression(recordIdId)));
        pred.conjunctWith(new Term(new FieldNameExpression("blockid"), OP_EQ, new ConstantExpression(blockId)));
        pred.conjunctWith(new Term(new FieldNameExpression("filename"), OP_EQ, new ConstantExpression(fileName)));
        ModifyData md = new ModifyData(tbl, map, pred);
        int updateCount = iup.executeModify(md, tx);
        if(updateCount == 0){
            List<String> fields = Arrays.asList("groupid", "recordid", "blockid", "filename");
            List<Constant> vals = Arrays.asList(groupId, recordIdId, blockId, fileName);
            InsertData ind = new InsertData(tbl, fields, vals);
            iup.executeInsert(ind, tx);
        }
    }
    public Constant queryGroup(RecordId recordId, Transaction tx){
        Constant recordIdId = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(recordId.id()));
        Constant blockId = Constant.newInstance(Type.INTEGER, ByteHelper.toBytes(recordId.block().number()));
        Constant fileName = Constant.newInstance(Type.INTEGER, recordId.block().fileName().getBytes());
        TablePlan tp = new TablePlan(tbl, tx);
        Predicate pred = new Predicate(new Term(new FieldNameExpression("recordid"), OP_EQ, new ConstantExpression(recordIdId)));
        pred.conjunctWith(new Term(new FieldNameExpression("blockid"), OP_EQ, new ConstantExpression(blockId)));
        pred.conjunctWith(new Term(new FieldNameExpression("filename"), OP_EQ, new ConstantExpression(fileName)));
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

    public List<RecordId> queryRecord(Constant groupId, Transaction tx){
        TablePlan tp = new TablePlan(tbl, tx);
        Predicate pred = new Predicate(new Term(new FieldNameExpression("groupid"), OP_EQ, new ConstantExpression(groupId)));
        Plan selectPlan = IndexSelector.selectByBestMatchedIndex(tbl, tp, pred, tx);
        if (selectPlan == null)
            selectPlan = new SelectPlan(tp, pred);
        else
            selectPlan = new SelectPlan(selectPlan, pred);
        Scan s = selectPlan.open();
        s.beforeFirst();
        List<RecordId> reList = new ArrayList<RecordId>();
        while (s.next()){
            String filename = (String)s.getVal("filename").asJavaVal();
            long blockId = (Long)s.getVal("blockid").asJavaVal();
            reList.add(new RecordId(new BlockId(filename, blockId), (int)s.getVal("recordid").asJavaVal()));
        }
        s.close();
        return  reList;
    }
    public VectorConstant getVec(RecordId recordId, Transaction tx){
        RecordFile rf = new RecordFile(ti, tx, true);
        rf.moveToRecordId(recordId);
        Constant reCon = rf.getVal(embField);
        VectorConstant reVal = (VectorConstant)reCon.asJavaVal();
        rf.close();
        return reVal;
    }

}
