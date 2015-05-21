package org.fao.fenix.wds.web.rest.fenix;

import org.fao.fenix.wds.core.bean.DatasourceBean;
import org.fao.fenix.wds.core.datasource.DatasourcePool;
import org.fao.fenix.wds.core.fenix.WDSUtilsMongoDB;
import org.fao.fenix.wds.core.fenix.WDSUtilsOrientDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * @author <a href="mailto:guido.barbaglia@fao.org">Guido Barbaglia</a>
 * @author <a href="mailto:guido.barbaglia@gmail.com">Guido Barbaglia</a>
 */
@Component
@Path("/fenix")
public class WDS {

    @Autowired
    private DatasourcePool datasourcePool;

    @Autowired
    private WDSUtilsOrientDB wdsUtilsOrientDB;

    @Autowired
    private WDSUtilsMongoDB wdsUtilsMongoDB;

    @GET
    @Path("/retrieve")
    public Response retrieve(@QueryParam("datasource") String datasource,
                             @QueryParam("query") final String query,
                             @QueryParam("collection") final String collection,
                             @DefaultValue("object") @QueryParam("outputType") final String outputType) throws Exception {



        /* Create datasource bean. */
        final DatasourceBean ds = datasourcePool.getDatasource(datasource);

        /* Check permissions. */
        if (ds.isRetrieve()) {

            /* Output stream. */
            StreamingOutput stream = null;

            /* Handle the request according to DB type. */
            switch (ds.getDriver()) {

                case MONGODB:
                    stream = wdsUtilsMongoDB.retrieve(ds, query, collection);
                    break;

                case ORIENTDB:
                    stream = wdsUtilsOrientDB.retrieve(ds, query, collection);
                    break;

                default:
                    if (outputType.equalsIgnoreCase("object"))
                        stream = WDSUtils.sqlStreamingOutputObject(ds, query);
                    else if (outputType.equalsIgnoreCase("array"))
                        stream = WDSUtils.sqlStreamingOutputArray(ds, query);
                    break;

            }

            /* Stream result */
            return Response.status(200).entity(stream).build();

        }

        /* Return message error otherwise. */
        else {
            throw new Exception("This datasource has no RETRIEVE privilege.");
        }

    }

    @POST
    @Path("/create")
    public Response create(@FormParam("datasource") String datasource,
                           @FormParam("query") final String query,
                           @FormParam("collection") final String collection,
                           @DefaultValue("object") @FormParam("outputType") final String outputType) throws Exception {

        /* Create datasource bean. */
        final DatasourceBean ds = datasourcePool.getDatasource(datasource);

        /* Check permissions. */
        if (ds.isCreate()) {

            /* ID of the new resource. */
            List<String> ids = null;

            /* Handle the request according to DB type. */
            switch (ds.getDriver()) {

                case MONGODB:
                    ids = wdsUtilsMongoDB.create(ds, query, collection);
                    break;

                case ORIENTDB:
                    ids = wdsUtilsOrientDB.create(ds, query, collection);
                    break;

                default:
                    ids = WDSUtils.sqlInsert(ds, query, collection);
                    break;

            }

            /* Create the output. */
            String out = "[";
            if (outputType.equalsIgnoreCase("object")) {
                for (int i = 0 ; i < ids.size() ; i++) {
                    out += "{\"id\": \"" + ids.get(i) + "\"}";
                    if (i < ids.size() - 1)
                        out += ",";
                }
            } else {
                for (int i = 0 ; i < ids.size() ; i++) {
                    out += "\"" + ids.get(i) + "\"";
                    if (i < ids.size() - 1)
                        out += ",";
                }
            }
            out += "]";

            /* Stream result */
            return Response.status(200).entity(out).build();

        }

        /* Return message error otherwise. */
        else {
            throw new Exception("This datasource has no CREATE privilege.");
        }

    }

}