package org.fao.fenix.wds.core.fenix;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.fao.fenix.wds.core.bean.DatasourceBean;
import org.fao.fenix.wds.core.jdbc.MongoDBConnectionManager;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:guido.barbaglia@fao.org">Guido Barbaglia</a>
 * @author <a href="mailto:guido.barbaglia@gmail.com">Guido Barbaglia</a>
 * */
public class WDSUtilsMongoDB implements WDSUtils {

    public List<String> create(DatasourceBean ds, String documents, String collection) throws Exception {

        /* Insert the documents. */
        MongoDBConnectionManager mgr = MongoDBConnectionManager.getInstance();
        Mongo mongo = mgr.getMongo();
        DB db = mongo.getDB(ds.getDbName());
        DBCollection dbCollection = db.getCollection(collection);
        List<DBObject> dbobjs = (List<DBObject>) JSON.parse(documents);
        dbCollection.insert(dbobjs);

        /* Prepare the output. */
        List<String> ids = new ArrayList<String>();
        for (DBObject dbobj : dbobjs)
            ids.add(dbobj.get("_id").toString());

        /* Return the output. */
        return ids;

    }

    public StreamingOutput retrieve(final DatasourceBean ds,
                                    final String query,
                                    final String collection,
                                    final String outputType) {

        return new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                /* Query MongoDB. */
                MongoDBConnectionManager mgr = MongoDBConnectionManager.getInstance();
                Mongo mongo = mgr.getMongo();
                DB db = mongo.getDB(ds.getDbName());
                DBCollection dbCollection = db.getCollection(collection);
                DBObject dbobj;
                DBCursor cursor;
                try {
                    dbobj = (DBObject) JSON.parse(query);
                    cursor = dbCollection.find(dbobj);
                } catch (Exception e) {
                    cursor = dbCollection.find();
                }


                /* Compute result. */
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                /* ...as an array of objects... */
                if (outputType.equalsIgnoreCase("object")) {
                    try {
                        writer.write("[");
                        int count = 0;
                        while (cursor.hasNext()) {
                            writer.write(cursor.next().toString());
                            if (count < cursor.size() - 1)
                                writer.write(",");
                            count++;
                        }
                    } finally {
                        cursor.close();
                        writer.write("]");
                    }
                }

                /* ...or as an array of arrays. */
                if (outputType.equalsIgnoreCase("array")) {
                    StringBuilder sb = new StringBuilder();
                    try {
                        sb.append("[");
                        int row = 0;
                        while (cursor.hasNext()) {
                            DBObject tmp = cursor.next();
                            int col = 0;
                            sb.append("[");
                            for (String key : tmp.keySet()) {
                                sb.append("\"").append(tmp.get(key).toString()).append("\"");
                                if (col++ < tmp.keySet().size() - 1)
                                    sb.append(",");
                            }
                            sb.append("]");
                            if (row++ < cursor.size() - 1)
                                sb.append(",");
                        }
                        sb.append("]");
                    } finally {
                        cursor.close();
                        writer.write(sb.toString());
                    }
                }

                /* Convert and write the output on the stream. */
                writer.flush();

            }

        };

    }

}
