/**
 *
 * FENIX (Food security and Early warning Network and Information Exchange)
 *
 * Copyright (c) 2011, by FAO of UN under the EC-FAO Food Security
 Information for Action Programme
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.fao.fenix.wds.web.rest.faostat;

import com.google.gson.Gson;
import org.fao.fenix.wds.core.bean.DatasourceBean;
import org.fao.fenix.wds.core.bean.NestedWhereBean;
import org.fao.fenix.wds.core.bean.SQLBean;
import org.fao.fenix.wds.core.constant.SQL;
import org.fao.fenix.wds.core.datasource.DatasourcePool;
import org.fao.fenix.wds.core.exception.WDSExceptionStreamWriter;
import org.fao.fenix.wds.core.jdbc.JDBCIterable;
import org.fao.fenix.wds.core.sql.Bean2SQL;
import org.fao.fenix.wds.core.utils.Wrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author <a href="mailto:guido.barbaglia@fao.org">Guido Barbaglia</a>
 * @author <a href="mailto:guido.barbaglia@gmail.com">Guido Barbaglia</a> 
 * */
@Component
@Path("/exporter")
public class FAOSTATExporter {

    @Autowired
    private Wrapper wrapper;

    @Autowired
    DatasourcePool datasourcePool;

    @POST
    @Path("/streamcsv")
    public Response streamCSV(final @FormParam("datasource_WQ_csv") String datasource,
                              final @FormParam("json_WQ_csv") String json,
                              final @FormParam("cssFilename_WQ_csv") String cssFilename,
                              final @FormParam("valueIndex_WQ_csv") String valueIndex,
                              final @FormParam("thousandSeparator_WQ_csv") String thousandSeparator,
                              final @FormParam("decimalSeparator_WQ_csv") String decimalSeparator,
                              final @FormParam("decimalNumbers_WQ_csv") String decimalNumbers,
                              final @FormParam("quote_WQ_csv") String quote,
                              final @FormParam("title_WQ_csv") String title,
                              final @FormParam("subtitle_WQ_csv") String subtitle) {

        System.out.println("WE ARE IN LOCALHOST");
        System.out.println(valueIndex);
        System.out.println(json);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Gson g = new Gson();
                SQLBean sql = g.fromJson(json, SQLBean.class);
                System.out.println(sql.getQuery());
                System.out.println();
                DatasourceBean db = datasourcePool.getDatasource(datasource.toUpperCase());
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // compute result
                JDBCIterable it = new JDBCIterable();

                try {

                    // alter the query to switch from LIMIT to TOP
                    if (datasource.toUpperCase().startsWith("FAOSTAT"))
                        sql.setQuery(replaceLimitWithTop(sql));

                    System.out.println(Bean2SQL.convert(sql));
                    it.query(db, Bean2SQL.convert(sql).toString());


                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamCSV' thrown an error: " + e.getMessage()));
                } catch (InstantiationException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamCSV' thrown an error: " + e.getMessage()));
                } catch (SQLException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamCSV' thrown an error: " + e.getMessage()));
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamCSV' thrown an error: " + e.getMessage()));
                } catch (Exception e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamCSV' thrown an error: " + e.getMessage()));
                }

                writer.write("\ufeff");

                List<String> cols = it.getColumnNames();
                for (int i = 0; i < cols.size(); i++) {
                    writer.write(cols.get(i));
                    if (i < cols.size() - 1)
                        writer.write(",");
                }
                writer.write("\n");

                // write the result of the query
                while(it.hasNext()) {
                    List<String> l = it.next();
                    for (int i = 0; i < l.size(); i++) {
                        writer.write("\"" + l.get(i) + "\"");
                        if (i < l.size() - 1)
                            writer.write(",");
                    }
                    writer.write("\n");
                }

                writer.write("\n");
                writer.write(createMetadata());
                writer.write("\n");

                // Convert and write the output on the stream
                writer.flush();
                writer.close();

            }

        };

        // Wrap result
        ResponseBuilder builder = Response.ok(stream);
        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".csv");

        // Stream Excel
        return builder.build();

    }

    @POST
    @Path("/streamexcel")
    public Response streamExcel(final @FormParam("datasource_WQ") String datasource,
                                final @FormParam("json_WQ") String json,
                                final @FormParam("cssFilename_WQ") String cssFilename,
                                final @FormParam("valueIndex_WQ") String valueIndex,
                                final @FormParam("thousandSeparator_WQ") String thousandSeparator,
                                final @FormParam("decimalSeparator_WQ") String decimalSeparator,
                                final @FormParam("decimalNumbers_WQ") String decimalNumbers,
                                final @FormParam("quote_WQ") String quote,
                                final @FormParam("title_WQ") String title,
                                final @FormParam("subtitle_WQ") String subtitle) {

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Gson g = new Gson();
                SQLBean sql = g.fromJson(json, SQLBean.class);
                DatasourceBean db = datasourcePool.getDatasource(datasource.toUpperCase());
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // compute result
                JDBCIterable it = new JDBCIterable();

                try {

                    // alter the query to switch from LIMIT to TOP
                    if (datasource.toUpperCase().startsWith("FAOSTAT"))
                        sql.setQuery(replaceLimitWithTop(sql));

                    it.query(db, Bean2SQL.convert(sql).toString());


                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamExcel' thrown an error: " + e.getMessage()));
                } catch (InstantiationException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamExcel' thrown an error: " + e.getMessage()));
                } catch (SQLException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamExcel' thrown an error: " + e.getMessage()));
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    WDSExceptionStreamWriter.streamException(os, ("Method 'streamExcel' thrown an error: " + e.getMessage()));
                } catch (Exception e) {
                    WDSExceptionStreamWriter.streamException(os, ("Method 'getDomains' thrown an error: " + e.getMessage()));
                }

                writer.write("\ufeff");
                writer.write("<table>");

                // add column names
                List<String> cols = it.getColumnNames();
                writer.write("<tr>");
                for (int i = 0; i < cols.size(); i++) {
                    writer.write("<td>");
                    writer.write(cols.get(i));
                    writer.write("</td>");
                }
                writer.write("</tr>");

                // write the result of the query
                while(it.hasNext()) {
                    List<String> l = it.next();
                    writer.write("<tr>");
                    for (int i = 0; i < l.size(); i++) {
                        writer.write("<td>");
                        writer.write(l.get(i));
                        writer.write("</td>");
                    }
                    writer.write("</tr>");
                }

                // add metadata
                writer.write("<tr><td>&nbsp;</td></tr>");
                writer.write("<tr>");
                writer.write("<td>");
                writer.write(createMetadata());
                writer.write("<td>");
                writer.write("</tr>");

                writer.write("</table>");

                // Convert and write the output on the stream
                writer.flush();
                writer.close();

            }

        };

        // Wrap result
        ResponseBuilder builder = Response.ok(stream);
        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".xls");

        // Stream Excel
        return builder.build();

    }

    private String createMetadata() {
        StringBuilder sb = new StringBuilder();
        sb.append("FAOSTAT Date: ").append(new Date());
        return sb.toString();
    }

    @POST
    @Path("/excel")
    public Response createExcel(@FormParam("data") final String data) {

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // initiate variables
                Gson g = new Gson();
                String[][] array = g.fromJson(data, String[][].class);
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // Create HTML to be converted in Excel
                StringBuilder sb = new StringBuilder();
                sb.append("<table>");
                for (int i = 0; i < array.length; i++) {
                    sb.append("<tr>");
                    String[] row = array[i];
                    for (String s : row)
                        sb.append("<td>").append(s).append("</td>");
                    sb.append("</tr>");
                }
                sb.append("</table>");

                // Write to the stream
                writer.write(sb.toString());
                writer.flush();

            }

        };

        // Wrap result
        ResponseBuilder builder = Response.ok(stream);
//        builder.header("Access-Control-Allow-Origin", "*");
//        builder.header("Access-Control-Max-Age", "3600");
//        builder.header("Access-Control-Allow-Methods", "GET");
//        builder.header("Access-Control-Allow-Headers", "X-Requested-With, Host, User-Agent, Accept, Accept-Language, Accept-Encoding, Accept-Charset, Keep-Alive, Connection, Referer,Origin");
        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".xls");

        // Stream Excel
        return builder.build();

    }

    @POST
    @Path("/htmltable")
    public Response createExcelFromHTML(@FormParam("data") final String data) {

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // Write to the stream
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                writer.write(data);
                writer.flush();

            }

        };

        // Wrap result
        ResponseBuilder builder = Response.ok(stream);
        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".xls");

        // Stream Excel
        return builder.build();

    }

    private String replaceLimitWithTop(SQLBean sql) {
        for (NestedWhereBean nwb : sql.getNestedWheres()) {
            SQLBean sql2 = nwb.getNestedCondition();
            String script2 = Bean2SQL.convert(sql2).toString();
            if (sql2.getLimit() != null && sql2.getLimit().length() > 0) {
                if (sql.getLimit() != SQL.NONE.name()) {
                    script2 = script2.replaceFirst("SELECT ", "SELECT TOP " + sql2.getLimit() + " ");
                    script2 = script2.replaceFirst("LIMIT " + sql2.getLimit(), "");
                }
            }
            sql2.setQuery(script2);
        }
        String script = Bean2SQL.convert(sql).toString();
        if (sql.getLimit() != null && sql.getLimit().length() > 0) {
            if (sql.getLimit() != SQL.NONE.name()) {
                script = script.replaceFirst("SELECT ", "SELECT TOP " + sql.getLimit() + " ");
                script = script.replaceFirst("LIMIT " + sql.getLimit(), "");
            }
        }
        return script;
    }

}