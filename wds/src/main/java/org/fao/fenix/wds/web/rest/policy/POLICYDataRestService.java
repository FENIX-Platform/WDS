package org.fao.fenix.wds.web.rest.policy;

import com.google.gson.Gson;
import com.mongodb.util.JSON;
import org.fao.fenix.wds.core.bean.DatasourceBean;
import org.fao.fenix.wds.core.bean.SQLBean;
import org.fao.fenix.wds.core.bean.policy.POLICYDataObject;
import org.fao.fenix.wds.core.datasource.DatasourcePool;
import org.fao.fenix.wds.core.exception.WDSExceptionStreamWriter;
import org.fao.fenix.wds.core.jdbc.JDBCIterable;
import org.fao.fenix.wds.core.policy.JDBCIterablePolicy;
import org.fao.fenix.wds.core.policy.PolicyProcedures;
import org.fao.fenix.wds.core.sql.Bean2SQL;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.*;

/**
 * @author <a href="mailto:barbara.cintoli@fao.org">Barbara Cintoli</a>
 * @author <a href="mailto:barbara.cintoli@gmail.com">Barbara Cintoli</a>
 */
@Component
@Path("/policyservice")
public class POLICYDataRestService {

    @Autowired
    private DatasourcePool datasourcePool;

    private static String bracketRegex = "\\(-\\)";

//    private FAOSTATProcedures fp = new FAOSTATProcedures();

    private PolicyProcedures pp = new PolicyProcedures();

    private final Gson g = new Gson();
    private final Gson g1 = new Gson();

    //Policy at a Glance Functions Start
//    @POST
//    @Produces(MediaType.APPLICATION_JSON)
//    @Path("/biofuelPoliciesTimeSeries")
//    public Response biofuelPolicies_timeSeries(@FormParam("pdObj") String pdObject) throws Exception {
//         System.out.println("biofuelPolicies_timeSeries start "+pdObject);
//        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
//        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
//        //The format of cplId is this: 'code1', 'code2', 'code3'
//        final Map<String, LinkedHashMap<String, String>> map=  pp.biofuelPolicies_timeSeries(dsBean, pd_obj);
//
//        for( String key: map.keySet())
//        {
//            System.out.println("key "+key);
//            if(key!=null)
//            {
//                Set<String> keySet2= map.get(key).keySet();
//                for(String key2: keySet2)
//                {
//                    System.out.println("time key2 "+key2);
//                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                    Date date = df.parse(key2);
////                    Date date = new Date(key2);
//                    long epoch = date.getTime();
//                    System.out.println(epoch);
//                    System.out.println("country count key "+map.get(key).get(key2));
//                }
//            }
//
//        }
//
//
//        final JDBCIterablePolicy it = new JDBCIterablePolicy();
//        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
//        // Initiate the stream
//        StreamingOutput stream = new StreamingOutput() {
//
//            @Override
//            public void write(OutputStream os) throws IOException, WebApplicationException {
//
//                // compute result
//                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
//
//                for( String key: map.keySet())
//                {
//                    writer.write("[");
//                    Set<String> keySet2= map.get(key).keySet();
//                    int i=0;
//                    for(String key2: keySet2) {
//                        System.out.println("time key2 "+key2);
//                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                        Date date = null;
//                        try {
//                            date = df.parse(key2);
//                        } catch (ParseException e) {
//                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                        }
////                    Date date = new Date(key2);
//                        long epoch = date.getTime();
//                        System.out.println(epoch);
//                        System.out.println("country count key "+map.get(key).get(key2));
//                        writer.write(g1.toJson("["+epoch+","+map.get(key).get(key2)+"]"));
//                        i++;
//                        if (i< keySet2.size())
//                            writer.write(",");
//                    }
//                   // writer.write("]");
//                    writer.write("]");
//                }
//
//                // Convert and write the output on the stream
//                writer.flush();
//            }
//        };
//
////        var seriesOptions = [];
////
////        seriesOptions.push({
////                name: '1',
////                data: [
////            /* May 2006 */
////        [1147651200000,67.79],
////        [1147737600000,64.98],
////        [1147824000000,65.26],
////        [1147910400000,63.18],
//
////        final JDBCIterablePolicy it = new JDBCIterablePolicy();
////        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
////        // Initiate the stream
////        StreamingOutput stream = new StreamingOutput() {
////
////            @Override
////            public void write(OutputStream os) throws IOException, WebApplicationException {
////
////                // compute result
////                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
////
////                writer.write("[");
////                while(it.hasNext()) {
////                    writer.write(g.toJson(it.next()));
////                    if (it.hasNext())
////                        writer.write(",");
////                }
////                writer.write("]");
////
////                // Convert and write the output on the stream
////                writer.flush();
////            }
////        };
//
//        // Stream result
//        return Response.status(200).entity(stream).build();
//    }
    //Policy at a Glance Functions Start
    //Highstock Chart
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/biofuelPoliciesTimeSeries")
    public Response biofuelPolicies_timeSeries(@FormParam("pdObj") String pdObject) throws Exception {
        // System.out.println("biofuelPolicies_timeSeries start "+pdObject);
        final POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final Map<String, LinkedHashMap<String, String>> map=  pp.biofuelPolicies_timeSeries(dsBean, pd_obj);
        //final Map<String, LinkedHashMap<String, String>> map=  pp.biofuelPolicies_timeSeriesExcel(dsBean, pd_obj);

        final JDBCIterablePolicy it = new JDBCIterablePolicy();
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                JSONArray jsonArrayTot = new JSONArray();
                int series_count = 1;
                //For each policy type
                Date dStart = new Date();
                Date dEnd = new Date();

                try {
                    dStart = new SimpleDateFormat("yyyy-MM-dd").parse("2011-01-01");
//                    dEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01");
                    dEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2014-12-31");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                for( String key: map.keySet())
                {
                    //writer.write("[");
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("name", series_count);
                    JSONArray jsonArray = new JSONArray();
                    Set<String> keySet2= map.get(key).keySet();
                    int i=0;
                    for(String key2: keySet2) {
                        //System.out.println("time key2 "+key2);
//                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM");
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = null;
                        try {
                            date = df.parse(key2);
                        } catch (ParseException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
//                    Date date = new Date(key2);
                        long epoch = date.getTime();
                        //System.out.println("country count key "+map.get(key).get(key2));
                        //writer.write(g1.toJson("["+epoch+","+map.get(key).get(key2)+"]"));

//                        if((date.before(dEnd))&&(date.after(dStart))){
                        if((date.compareTo(dEnd)<=0)&&(date.compareTo(dStart)>=0)){
                            //System.out.println(date);
                            JSONArray jsonArrayInside = new JSONArray();
                            jsonArrayInside.add(epoch);
                            jsonArrayInside.add(Integer.parseInt(map.get(key).get(key2)));
                            jsonArray.add(jsonArrayInside);
                            i++;
                        }
//                        if (i< keySet2.size())
//                            writer.write(",");
                    }

                    jsonobj.put("data", jsonArray);
                    // writer.write("]");
//                    writer.write("]");
                    jsonArrayTot.add(jsonobj);
                    series_count++;
                }
                //System.out.println("Max EPOCH "+ maxEpoch);
                JSONObject jsonobj_to_return = new JSONObject();

                jsonobj_to_return.put("commodityClassCode",pd_obj.getCommodity_class_code());
                jsonobj_to_return.put("dataArray",jsonArrayTot);
                writer.write(g.toJson(jsonobj_to_return));
//                writer.write(g.toJson(jsonArrayTot));

                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }


    @POST
    @Path("/biofuelPoliciesTimeSeriesExcel")
    //'AllData'= Master+Policy tables
    public Response biofuelPoliciesTimeSeriesExcel(final @FormParam("datasource_PT") String datasource,
                                final @FormParam("json_PT") String json,
                                final @FormParam("cssFilename_PT") String cssFilename,
                                final @FormParam("valueIndex_PT") String valueIndex,
                                final @FormParam("thousandSeparator_PT") String thousandSeparator,
                                final @FormParam("decimalSeparator_PT") String decimalSeparator,
                                final @FormParam("decimalNumbers_PT") String decimalNumbers,
                                final @FormParam("quote_PT") String quote,
                                final @FormParam("title_PT") String title,
                                final @FormParam("subtitle_PT") String subtitle) {
        //DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
//        final Map<String, LinkedHashMap<String, String>> countryMap=  pp.biofuelPolicies_timeSeriesExcel(dsBean, pd_obj);

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                POLICYDataObject pd_obj = g.fromJson(json, POLICYDataObject.class);
                DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                try {
                    String policy_type_code_array[]= pd_obj.getPolicy_type_code();
                    String policy_type_name_array[]= pd_obj.getPolicy_type_name();
                    final Map<String, LinkedHashMap<String, String>>[] countryMapArray=  pp.biofuelPolicies_timeSeriesExcel(dsBean, pd_obj);
                    String[] headerArray = {"Period","Policy Type", "Sum of countries", "Argentina", "Australia", "Brazil", "Canada", "China", "Egypt", "European Union", "France", "Germany","India","Indonesia", "Italy", "Japan", "Kazakhstan", "Mexico", "Nigeria", "Philippines","Republic of Korea", "Russian Federation","Saudi Arabia","South Africa", "Spain", "Thailand", "Turkey", "United Kingdom", "Ukraine","United States of America","Viet Nam"};
                    String[] headerArrayKey = {"SUM", "Argentina", "Australia", "Brazil", "Canada", "China", "Egypt", "European Union", "France", "Germany","India","Indonesia", "Italy", "Japan", "Kazakhstan", "Mexico", "Nigeria", "Philippines","Republic of Korea", "Russian Federation","Saudi Arabia","South Africa", "Spain", "Thailand", "Turkey", "United Kingdom", "Ukraine","United States of America","Viet Nam"};

                    // write the result of the query
                    writer.write("<html><head><meta charset=\"UTF-8\"></head><body>");
//                DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
//                Date date = new Date();
                    //System.out.println(dateFormat.format(date));

                    writer.write("<table border=\"1\">");
                    // System.out.println("Table");
//                writer.write("<tr>");
//                writer.write("<td><b>");
//                writer.write("Date:");
//                writer.write("</b></td>");
//                writer.write("<td>");
//                writer.write(dateFormat.format(date));
//                writer.write("</td>");
//                writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("<td><b>");
                    writer.write("Source:");
                    writer.write("</b></td>");
                    writer.write("<td>");
                    writer.write("AMIS Policy Database");
                    writer.write("</td>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("<td><b>");
                    writer.write(title);
                    writer.write("</b></td>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    for (int i = 0; i < headerArray.length; i++) {
                        writer.write("<td><b>");
                        writer.write(headerArray[i]);
                        writer.write("</b></td>");
                    }
                    writer.write("</tr>");
//                    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

                    for(int j =0; j<policy_type_code_array.length; j++){
                        Map<String, LinkedHashMap<String, String>> countryMap= countryMapArray[j];
                        for(String key: countryMap.keySet())
                        {
//                    Set<String> keySet2= countryMap.get(key).keySet();
                            writer.write("<tr>");
                            //Period
                            writer.write("<td nowrap>");
                            //System.out.println("KEY!!!!!!!!!!");
                            //System.out.println(key);
                            GregorianCalendar date = new GregorianCalendar(TimeZone.getTimeZone(key));
                            //System.out.println(dateFormat.format(date));
                            //writer.write(dateFormat.format(date));
                            writer.write(key);
                            writer.write("</td>");
                            //Policy Type Name
                            writer.write("<td nowrap>");
                            writer.write(policy_type_name_array[j]);
                            writer.write("</td>");
                            for (int i = 0; i < headerArrayKey.length; i++) {
                                writer.write("<td nowrap>");
//                        if((i==15)||(i==16))
//                        {
//                            //Short Description and Description  .... no wrap
//                            writer.write("<td nowrap>");
//                        }
//                        else{
//                            writer.write("<td>");
//                        }
                                //writer.write("<td>");
                                String app = countryMap.get(key).get(headerArrayKey[i]);
                                if((app==null)||(app.isEmpty()))
                                {
                                    writer.write("");
                                }
                                else
                                {
                                    writer.write(app);
                                }
                                writer.write("</td>");
                            }
                            writer.write("</tr>");
                        }
                    }
                    writer.write("</table>");
                    writer.write("</body></html>");
                    //System.out.println("Before writer");
                    // System.out.println(writer.toString());
                    // Convert and write the output on the stream
                    writer.flush();
                    writer.close();

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
            }
        };

        // Wrap result
        Response.ResponseBuilder builder = Response.ok(stream);
//        builder.header("Access-Control-Allow-Origin", "*");
//        builder.header("Access-Control-Max-Age", "3600");
//        builder.header("Access-Control-Allow-Methods", "POST");
//        builder.header("Access-Control-Allow-Headers", "X-Requested-With, Host, User-Agent, Accept, Accept-Language, Accept-Encoding, Accept-Charset, Keep-Alive, Connection, Referer,Origin");
//        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".xls");

        builder.header("Content-Disposition", "attachment; filename= PolicyData_" + UUID.randomUUID().toString() + ".xls");
        builder.header("Content-type",  "application/vnd.ms-excel; charset=UTF-8");
        // Stream Excel
        //System.out.println("Before build");
        return builder.build();
    }

    //Highstock Chart
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/biofuelPoliciesMeasuresTimeSeries")
    public Response biofuelPolicyMeasures_timeSeries(@FormParam("pdObj") String pdObject) throws Exception {
        // System.out.println("biofuelPolicies_timeSeries start "+pdObject);
        final POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final Map<String, LinkedHashMap<String, String>> map=  pp.biofuelPolicyMeasures_timeSeries(dsBean, pd_obj);

//        for( String key: map.keySet())
//        {
//            System.out.println("key "+key);
//            if(key!=null)
//            {
//                Set<String> keySet2= map.get(key).keySet();
//                for(String key2: keySet2)
//                {
//                    System.out.println("time key2 "+key2);
//                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                    Date date = df.parse(key2);
////                    Date date = new Date(key2);
//                    long epoch = date.getTime();
//                    System.out.println(epoch);
//                    System.out.println("country count key "+map.get(key).get(key2));
//                }
//            }
//        }

        final JDBCIterablePolicy it = new JDBCIterablePolicy();
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                Date dStart = new Date();
                Date dEnd = new Date();

                try {
                    dStart = new SimpleDateFormat("yyyy-MM-dd").parse("2011-01-01");
//                    dEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01");
                    dEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2014-12-31");
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                JSONArray jsonArrayTot = new JSONArray();
                int series_count = 1;
                //For each policy type
                for( String key: map.keySet())
                {
                    //writer.write("[");
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("name", series_count);
                    JSONArray jsonArray = new JSONArray();
                    Set<String> keySet2= map.get(key).keySet();
                    int i=0;
                    for(String key2: keySet2) {
                        //System.out.println("time key2 "+key2);
//                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM");
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = null;
                        try {
//                            if(series_count==1){
//                                System.out.println(key2 + " VALUE= "+Integer.parseInt(map.get(key).get(key2)));
//                            }
                            date = df.parse(key2);
                        } catch (ParseException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
//                    Date date = new Date(key2);

                        long epoch = date.getTime();
                        //System.out.println(epoch);
                        //System.out.println("country count key "+map.get(key).get(key2));
                        //writer.write(g1.toJson("["+epoch+","+map.get(key).get(key2)+"]"));
                        if((date.compareTo(dEnd)<=0)&&(date.compareTo(dStart)>=0)) {
                            JSONArray jsonArrayInside = new JSONArray();
                            jsonArrayInside.add(epoch);
                            jsonArrayInside.add(Integer.parseInt(map.get(key).get(key2)));
                            jsonArray.add(jsonArrayInside);
                            i++;
                        }
//                        if (i< keySet2.size())
//                            writer.write(",");
                    }

                    jsonobj.put("data", jsonArray);
                    // writer.write("]");
//                    writer.write("]");
                    jsonArrayTot.add(jsonobj);
                    series_count++;
                }
                JSONObject jsonobj_to_return = new JSONObject();
                jsonobj_to_return.put("commodityClassCode",pd_obj.getCommodity_class_code());
                jsonobj_to_return.put("dataArray",jsonArrayTot);
                writer.write(g.toJson(jsonobj_to_return));
//                writer.write(g.toJson(jsonArrayTot));

                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();


    }

    //Highstock Chart
    @POST
    @Path("/biofuelPoliciesMeasuresTimeSeriesExcel")
    //'AllData'= Master+Policy tables
    public Response biofuelPolicyMeasures_timeSeriesExcel(final @FormParam("datasource_PM") String datasource,
                                                   final @FormParam("json_PM") String json,
                                                   final @FormParam("cssFilename_PM") String cssFilename,
                                                   final @FormParam("valueIndex_PM") String valueIndex,
                                                   final @FormParam("thousandSeparator_PM") String thousandSeparator,
                                                   final @FormParam("decimalSeparator_PM") String decimalSeparator,
                                                   final @FormParam("decimalNumbers_PM") String decimalNumbers,
                                                   final @FormParam("quote_PM") String quote,
                                                   final @FormParam("title_PM") String title,
                                                   final @FormParam("subtitle_PM") String subtitle) {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                System.out.println("biofuelPolicyMeasures_timeSeriesExcel start");

                final POLICYDataObject pd_obj = g.fromJson(json, POLICYDataObject.class);
                System.out.println(json);
                System.out.println(pd_obj);
                DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
                //The format of cplId is this: 'code1', 'code2', 'code3'

                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                try {
                    String policy_type_code_array[]= pd_obj.getPolicy_type_code();
                    String policy_measure_code_array[]= pd_obj.getPolicy_measure_code();
                    String policy_measure_name_array[]= pd_obj.getPolicy_measure_name();
                    String policy_type_name_array[]= pd_obj.getPolicy_type_name();
                    //policy_type_measure_info_name= PT CODE = 1 -> Object { 4="Export tax",  5="Licensing requirement",  10="VAT tax rebate",  altri elementi...}
                    LinkedHashMap<String, LinkedHashMap<String, String>> policy_type_measure_info_name = pd_obj.getPolicyTypesMeasureInfoName();
//                    System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                    System.out.println(policy_type_measure_info_name);
//                    String[][] policy_type_measure_info = pd_obj.getPolicyTypesMeasureInfo();
//                    String[][] policy_type_measure_info_name = pd_obj.getPolicyTypesMeasureInfoName();
                    System.out.println(policy_type_code_array);
                    final Map<String, LinkedHashMap<String, String>>[][] countryMapArray=  pp.biofuelPolicyMeasures_timeSeriesExcel(dsBean, pd_obj);
                    String[] headerArray = {"Period", "Policy Type", "Policy Measure", "Sum of countries", "Argentina", "Australia", "Brazil", "Canada", "China", "Egypt", "European Union", "France", "Germany","India","Indonesia", "Italy", "Japan", "Kazakhstan", "Mexico", "Nigeria", "Philippines","Republic of Korea", "Russian Federation","Saudi Arabia","South Africa", "Spain", "Thailand", "Turkey", "United Kingdom", "Ukraine","United States of America","Viet Nam"};
                    String[] headerArrayKey = {"SUM", "Argentina", "Australia", "Brazil", "Canada", "China", "Egypt", "European Union", "France", "Germany","India","Indonesia", "Italy", "Japan", "Kazakhstan", "Mexico", "Nigeria", "Philippines","Republic of Korea", "Russian Federation","Saudi Arabia","South Africa", "Spain", "Thailand", "Turkey", "United Kingdom", "Ukraine","United States of America","Viet Nam"};

                    // write the result of the query
                    writer.write("<html><head><meta charset=\"UTF-8\"></head><body>");
//                DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
//                Date date = new Date();
                    //System.out.println(dateFormat.format(date));

                    writer.write("<table border=\"1\">");
                    // System.out.println("Table");
//                writer.write("<tr>");
//                writer.write("<td><b>");
//                writer.write("Date:");
//                writer.write("</b></td>");
//                writer.write("<td>");
//                writer.write(dateFormat.format(date));
//                writer.write("</td>");
//                writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("<td><b>");
                    writer.write("Source:");
                    writer.write("</b></td>");
                    writer.write("<td>");
                    writer.write("AMIS Policy Database");
                    writer.write("</td>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("<td><b>");
                    writer.write(title);
                    writer.write("</b></td>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    for (int i = 0; i < headerArray.length; i++) {
                        writer.write("<td><b>");
                        writer.write(headerArray[i]);
                        writer.write("</b></td>");
                    }
                    writer.write("</tr>");
                    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

                    for(int z =0; z<policy_type_code_array.length; z++) {
                        System.out.println("z= "+z+ "policy_type_code_array elem "+policy_type_code_array[z]);
//                    for(int z =0; z<1; z++) {
                       Map<String, LinkedHashMap<String, String>> countryMap_for_PT[]= countryMapArray[z];
                        LinkedHashMap<String, String> pm_infoNameMap = policy_type_measure_info_name.get(policy_type_code_array[z]);
                        Set<String> pm_keySet= pm_infoNameMap.keySet();
                        Iterator<String> it = pm_keySet.iterator();
                        //it and countryMap_for_PT have the same length
                        for(int j =0; j<countryMap_for_PT.length; j++){
//                       for(int j =0; j<1; j++){
//                        Map<String, LinkedHashMap<String, String>> countryMap= countryMapArray[j];
                            String s = it.next();
                            Map<String, LinkedHashMap<String, String>> countryMap= countryMap_for_PT[j];
                            for(String key: countryMap.keySet())
                            {
                                //System.out.println("KEY!!!!!");
                                //System.out.println(key);
//                    Set<String> keySet2= countryMap.get(key).keySet();
                                writer.write("<tr>");
                                //Period
                                writer.write("<td nowrap>");
                                GregorianCalendar date = new GregorianCalendar(TimeZone.getTimeZone(key));
                                //System.out.println("11111");
                                writer.write(key);
                                writer.write("</td>");
                                //Policy Type Name
                                writer.write("<td nowrap>");
                                //System.out.println("22222");
                                //System.out.println(policy_type_name_array[z]);
//                                writer.write(policy_measure_name_array[j]);
                                writer.write(policy_type_name_array[z]);
                                //System.out.println("policy_type_name_array[j]!!!!!");
                                //System.out.println(policy_type_name_array[j]);
                                writer.write("</td>");
//                                //Policy Measure Name
                                writer.write("<td nowrap>");
//                                //System.out.println("22222");
//                                //System.out.println(policy_type_name_array);
////                                writer.write(policy_measure_name_array[j]);
////                                writer.write(policy_type_measure_info_name[z][j]);

//                                System.out.println("SSSSSSSS");
//                                System.out.println(s);
////                                System.out.println("IN MAPPPPPP");
//                                System.out.println(pm_infoNameMap.get(s));
///                                System.out.println("IN MAPPPPPP END");
                                writer.write(pm_infoNameMap.get(s));
//                                //System.out.println("policy_type_name_array[j]!!!!!");
//                                //System.out.println(policy_type_name_array[j]);
                                writer.write("</td>");
                                for (int i = 0; i < headerArrayKey.length; i++) {
                                    writer.write("<td nowrap>");
                                    String app = countryMap.get(key).get(headerArrayKey[i]);
                                    //System.out.println("app= *"+app+"*");
                                    if((app==null)||(app.isEmpty()))
                                    {
                                        writer.write("");
                                    }
                                    else
                                    {
                                        writer.write(app);
                                    }
                                    writer.write("</td>");
                                }
                                writer.write("</tr>");
                            }
                        }
                    }

                    writer.write("</table>");
                    writer.write("</body></html>");
                    //System.out.println("Before writer");
                    // System.out.println(writer.toString());
                    // Convert and write the output on the stream
                    writer.flush();
                    writer.close();

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
            }
        };

        // Wrap result
        Response.ResponseBuilder builder = Response.ok(stream);
//        builder.header("Access-Control-Allow-Origin", "*");
//        builder.header("Access-Control-Max-Age", "3600");
//        builder.header("Access-Control-Allow-Methods", "POST");
//        builder.header("Access-Control-Allow-Headers", "X-Requested-With, Host, User-Agent, Accept, Accept-Language, Accept-Encoding, Accept-Charset, Keep-Alive, Connection, Referer,Origin");
//        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".xls");

        builder.header("Content-Disposition", "attachment; filename= PolicyData_" + UUID.randomUUID().toString() + ".xls");
        builder.header("Content-type",  "application/vnd.ms-excel; charset=UTF-8");
        // Stream Excel
        //System.out.println("Before build");
        return builder.build();
    }

    //Highstock Chart
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/exportRestrictionsPoliciesMeasuresTimeSeries")
    public Response exportRestrictionsPolicyMeasures_timeSeries(@FormParam("pdObj") String pdObject) throws Exception {
        // System.out.println("biofuelPolicies_timeSeries start "+pdObject);
        final POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final Map<String, LinkedHashMap<String, String>> map=  pp.exportRestrictionsPolicyMeasures_timeSeries(dsBean, pd_obj);

        final JDBCIterablePolicy it = new JDBCIterablePolicy();
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                Date dStart = new Date();
                Date dEnd = new Date();

                try {
                    dStart = new SimpleDateFormat("yyyy-MM-dd").parse("2007-01-01");
//                    dEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01");
                    dEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2014-12-31");
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                JSONArray jsonArrayTot = new JSONArray();
                int series_count = 1;
                //For each policy type
                for( String key: map.keySet())
                {
                    //writer.write("[");
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("name", series_count);
                    JSONArray jsonArray = new JSONArray();
                    Set<String> keySet2= map.get(key).keySet();
                    int i=0;
                    for(String key2: keySet2) {
                        //System.out.println("time key2 "+key2);
//                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM");
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date date = null;
                        try {
                            date = df.parse(key2);
                        } catch (ParseException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
//                    Date date = new Date(key2);
                        long epoch = date.getTime();
                        //System.out.println(epoch);
                        //System.out.println("country count key "+map.get(key).get(key2));
                        //writer.write(g1.toJson("["+epoch+","+map.get(key).get(key2)+"]"));
                        if((date.compareTo(dEnd)<=0)&&(date.compareTo(dStart)>=0)) {
                            JSONArray jsonArrayInside = new JSONArray();
                            jsonArrayInside.add(epoch);
                            jsonArrayInside.add(Integer.parseInt(map.get(key).get(key2)));
                            jsonArray.add(jsonArrayInside);
                            i++;
                        }
//                        if (i< keySet2.size())
//                            writer.write(",");
                    }

                    jsonobj.put("data", jsonArray);
                    // writer.write("]");
//                    writer.write("]");
                    jsonArrayTot.add(jsonobj);
                    series_count++;
                }
                JSONObject jsonobj_to_return = new JSONObject();
                jsonobj_to_return.put("commodityClassCode",pd_obj.getCommodity_class_code());
                jsonobj_to_return.put("dataArray",jsonArrayTot);
                writer.write(g.toJson(jsonobj_to_return));
//                writer.write(g.toJson(jsonArrayTot));

                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    //Highstock Chart
    @POST
    @Path("/exportRestrictionsTimeSeriesExcel")
    //'AllData'= Master+Policy tables
    public Response exportRestrictionsTimeSeriesExcel(final @FormParam("datasource_ER") String datasource,
                                                          final @FormParam("json_ER") String json,
                                                          final @FormParam("cssFilename_ER") String cssFilename,
                                                          final @FormParam("valueIndex_ER") String valueIndex,
                                                          final @FormParam("thousandSeparator_ER") String thousandSeparator,
                                                          final @FormParam("decimalSeparator_ER") String decimalSeparator,
                                                          final @FormParam("decimalNumbers_ER") String decimalNumbers,
                                                          final @FormParam("quote_ER") String quote,
                                                          final @FormParam("title_ER") String title,
                                                          final @FormParam("subtitle_ER") String subtitle) {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                System.out.println("exportRestrictionsTimeSeriesExcel start");

                final POLICYDataObject pd_obj = g.fromJson(json, POLICYDataObject.class);
                System.out.println(json);
                System.out.println(pd_obj);
                DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
                //The format of cplId is this: 'code1', 'code2', 'code3'

                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                try {
                    String policy_type_code_array[]= pd_obj.getPolicy_type_code();
                    String policy_measure_code_array[]= pd_obj.getPolicy_measure_code();
                    String policy_measure_name_array[]= pd_obj.getPolicy_measure_name();
                    String policy_type_name_array[]= pd_obj.getPolicy_type_name();
                    //policy_type_measure_info_name= PT CODE = 1 -> Object { 4="Export tax",  5="Licensing requirement",  10="VAT tax rebate",  altri elementi...}
                    LinkedHashMap<String, LinkedHashMap<String, String>> policy_type_measure_info_name = pd_obj.getPolicyTypesMeasureInfoName();
//                    System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                    System.out.println(policy_type_measure_info_name);
//                    String[][] policy_type_measure_info = pd_obj.getPolicyTypesMeasureInfo();
//                    String[][] policy_type_measure_info_name = pd_obj.getPolicyTypesMeasureInfoName();
                    System.out.println(policy_type_code_array);
                    final Map<String, LinkedHashMap<String, String>>[][] countryMapArray=  pp.exportRestrictions_timeSeriesExcel(dsBean, pd_obj);
                    String[] headerArray = {"Period", "Policy Type", "Policy Measure", "Sum of countries", "Argentina", "Australia", "Brazil", "Canada", "China", "Egypt", "European Union", "France", "Germany","India","Indonesia", "Italy", "Japan", "Kazakhstan", "Mexico", "Nigeria", "Philippines","Republic of Korea", "Russian Federation","Saudi Arabia","South Africa", "Spain", "Thailand", "Turkey", "United Kingdom", "Ukraine","United States of America","Viet Nam"};
                    String[] headerArrayKey = {"SUM", "Argentina", "Australia", "Brazil", "Canada", "China", "Egypt", "European Union", "France", "Germany","India","Indonesia", "Italy", "Japan", "Kazakhstan", "Mexico", "Nigeria", "Philippines","Republic of Korea", "Russian Federation","Saudi Arabia","South Africa", "Spain", "Thailand", "Turkey", "United Kingdom", "Ukraine","United States of America","Viet Nam"};

                    // write the result of the query
                    writer.write("<html><head><meta charset=\"UTF-8\"></head><body>");
//                DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
//                Date date = new Date();
                    //System.out.println(dateFormat.format(date));

                    writer.write("<table border=\"1\">");
                    // System.out.println("Table");
//                writer.write("<tr>");
//                writer.write("<td><b>");
//                writer.write("Date:");
//                writer.write("</b></td>");
//                writer.write("<td>");
//                writer.write(dateFormat.format(date));
//                writer.write("</td>");
//                writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("<td><b>");
                    writer.write("Source:");
                    writer.write("</b></td>");
                    writer.write("<td>");
                    writer.write("AMIS Policy Database");
                    writer.write("</td>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    writer.write("<td><b>");
                    writer.write(title);
                    writer.write("</b></td>");
                    writer.write("</tr>");

                    writer.write("<tr>");
                    for (int i = 0; i < headerArray.length; i++) {
                        writer.write("<td><b>");
                        writer.write(headerArray[i]);
                        writer.write("</b></td>");
                    }
                    writer.write("</tr>");
                    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

                    for(int z =0; z<policy_type_code_array.length; z++) {
                        System.out.println("z= "+z+ "policy_type_code_array elem "+policy_type_code_array[z]);
//                    for(int z =0; z<1; z++) {
                        Map<String, LinkedHashMap<String, String>> countryMap_for_PT[]= countryMapArray[z];
                        LinkedHashMap<String, String> pm_infoNameMap = policy_type_measure_info_name.get(policy_type_code_array[z]);
                        Set<String> pm_keySet= pm_infoNameMap.keySet();
                        Iterator<String> it = pm_keySet.iterator();
                        //it and countryMap_for_PT have the same length
                        for(int j =0; j<countryMap_for_PT.length; j++){
//                       for(int j =0; j<1; j++){
//                        Map<String, LinkedHashMap<String, String>> countryMap= countryMapArray[j];
                            String s = it.next();
                            Map<String, LinkedHashMap<String, String>> countryMap= countryMap_for_PT[j];
                            for(String key: countryMap.keySet())
                            {
                                //System.out.println("KEY!!!!!");
                                //System.out.println(key);
//                    Set<String> keySet2= countryMap.get(key).keySet();
                                writer.write("<tr>");
                                //Period
                                writer.write("<td nowrap>");
                                GregorianCalendar date = new GregorianCalendar(TimeZone.getTimeZone(key));
                                //System.out.println("11111");
                                writer.write(key);
                                writer.write("</td>");
                                //Policy Type Name
                                writer.write("<td nowrap>");
                                //System.out.println("22222");
                                //System.out.println(policy_type_name_array[z]);
//                                writer.write(policy_measure_name_array[j]);
                                writer.write(policy_type_name_array[z]);
                                //System.out.println("policy_type_name_array[j]!!!!!");
                                //System.out.println(policy_type_name_array[j]);
                                writer.write("</td>");
//                                //Policy Measure Name
                                writer.write("<td nowrap>");
//                                //System.out.println("22222");
//                                //System.out.println(policy_type_name_array);
////                                writer.write(policy_measure_name_array[j]);
////                                writer.write(policy_type_measure_info_name[z][j]);

//                                System.out.println("SSSSSSSS");
//                                System.out.println(s);
////                                System.out.println("IN MAPPPPPP");
//                                System.out.println(pm_infoNameMap.get(s));
///                                System.out.println("IN MAPPPPPP END");
                                writer.write(pm_infoNameMap.get(s));
//                                //System.out.println("policy_type_name_array[j]!!!!!");
//                                //System.out.println(policy_type_name_array[j]);
                                writer.write("</td>");
                                for (int i = 0; i < headerArrayKey.length; i++) {
                                    writer.write("<td nowrap>");
                                    String app = countryMap.get(key).get(headerArrayKey[i]);
                                    //System.out.println("app= *"+app+"*");
                                    if((app==null)||(app.isEmpty()))
                                    {
                                        writer.write("");
                                    }
                                    else
                                    {
                                        writer.write(app);
                                    }
                                    writer.write("</td>");
                                }
                                writer.write("</tr>");
                            }
                        }
                    }

                    writer.write("</table>");
                    writer.write("</body></html>");
                    //System.out.println("Before writer");
                    // System.out.println(writer.toString());
                    // Convert and write the output on the stream
                    writer.flush();
                    writer.close();

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
            }
        };

        // Wrap result
        Response.ResponseBuilder builder = Response.ok(stream);
//        builder.header("Access-Control-Allow-Origin", "*");
//        builder.header("Access-Control-Max-Age", "3600");
//        builder.header("Access-Control-Allow-Methods", "POST");
//        builder.header("Access-Control-Allow-Headers", "X-Requested-With, Host, User-Agent, Accept, Accept-Language, Accept-Encoding, Accept-Charset, Keep-Alive, Connection, Referer,Origin");
//        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".xls");

        builder.header("Content-Disposition", "attachment; filename= PolicyData_" + UUID.randomUUID().toString() + ".xls");
        builder.header("Content-type",  "application/vnd.ms-excel; charset=UTF-8");
        // Stream Excel
        //System.out.println("Before build");
        return builder.build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/policyTypesFromDomain/{datasource}/{commodityDomainCode}")
    public Response getpolicyTypes_fromPolicyDomain(@PathParam("datasource") String datasource, @PathParam("commodityDomainCode") String commodityDomainCode) throws Exception {

        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getpolicyTypes_fromPolicyDomain(dsBean, commodityDomainCode);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/exportSubsidiesCountries/{datasource}/{policyType}/{policyMeasure}")
    public Response getcountries_fromExportSubsidies(@PathParam("datasource") String datasource, @PathParam("policyType") String policyType, @PathParam("policyMeasure") String policyMeasure) throws Exception {

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getcountries_fromPolicy(dsBean, policyType, policyMeasure);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    //Highchart Chart
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/biofuelPoliciesBarChart")
    public Response biofuelPolicies_barchart(@FormParam("pdObj") String pdObject) throws Exception {
        System.out.println(pdObject);
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'

        final Map<String, LinkedHashMap<String, String>> map=  pp.biofuelPolicies_barchart(dsBean, pd_obj);
//        final Map<String, LinkedHashMap<String, String>> map= new HashMap<String, LinkedHashMap<String, String>>();
        //        for( String key: map.keySet())
//        {
//            System.out.println("key "+key);
//            if(key!=null)
//            {
//                Set<String> keySet2= map.get(key).keySet();
//                for(String key2: keySet2)
//                {
//                    System.out.println("time key2 "+key2);
//                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                    Date date = df.parse(key2);
////                    Date date = new Date(key2);
//                    long epoch = date.getTime();
//                    System.out.println(epoch);
//                    System.out.println("country count key "+map.get(key).get(key2));
//                }
//            }
//        }
        /*
        * series: [{
                        name: '1',
                        data: [49, 71, 106]
                    }, {
                        name: '2',
                        data: [93, 106, 84]
                    }]*/

        final JDBCIterablePolicy it = new JDBCIterablePolicy();
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                JSONArray jsonArrayTot = new JSONArray();
                int series_count = 1;
                for( String key: map.keySet())
                {
                    //One obj for each policy type
                    //writer.write("[");
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("name", series_count);
                    JSONArray jsonArray = new JSONArray();
                    Set<String> keySet2= map.get(key).keySet();

                    //Three elements... Ethanol, Biodisel, Biofuel
                    for(String key2: keySet2) {
                        jsonArray.add(Integer.parseInt(map.get(key).get(key2)));
                    }
                    jsonobj.put("data", jsonArray);
                    // writer.write("]");
//                    writer.write("]");
                    jsonArrayTot.add(jsonobj);
                    series_count++;
                }
                writer.write(g.toJson(jsonArrayTot));
                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    //Highchart Chart
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/biofuelPolicyMeasuresBarChart")
    public Response biofuelPolicyMeasures_barchart(@FormParam("pdObj") String pdObject) throws Exception {
        System.out.println(pdObject);
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final Map<String, LinkedHashMap<String, String>> map=  pp.biofuelMeasures_barchart(dsBean, pd_obj);
//        final Map<String, LinkedHashMap<String, String>> map= new HashMap<String, LinkedHashMap<String, String>>();
        //        for( String key: map.keySet())
//        {
//            System.out.println("key "+key);
//            if(key!=null)
//            {
//                Set<String> keySet2= map.get(key).keySet();
//                for(String key2: keySet2)
//                {
//                    System.out.println("time key2 "+key2);
//                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                    Date date = df.parse(key2);
////                    Date date = new Date(key2);
//                    long epoch = date.getTime();
//                    System.out.println(epoch);
//                    System.out.println("country count key "+map.get(key).get(key2));
//                }
//            }
//        }
        /*
        * series: [{
                        name: '1',
                        data: [49, 71, 106]
                    }, {
                        name: '2',
                        data: [93, 106, 84]
                    }]*/

        final JDBCIterablePolicy it = new JDBCIterablePolicy();
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                JSONArray jsonArrayTot = new JSONArray();
                int series_count = 1;
                for( String key: map.keySet())
                {
                    //One obj for each policy type
                    //writer.write("[");
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("name", series_count);
                    JSONArray jsonArray = new JSONArray();
                    Set<String> keySet2= map.get(key).keySet();

                    //Three elements... Ethanol, Biodisel, Biofuel
                    for(String key2: keySet2) {
                        jsonArray.add(Integer.parseInt(map.get(key).get(key2)));
                    }

                    jsonobj.put("data", jsonArray);
                    // writer.write("]");
//                    writer.write("]");
                    jsonArrayTot.add(jsonobj);
                    series_count++;
                }
                writer.write(g.toJson(jsonArrayTot));
                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    //Highchart Chart
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/exportRestrictionsPolicyMeasuresBarChart")
    public Response exportRestrictionsPolicyMeasures_barchart(@FormParam("pdObj") String pdObject) throws Exception {
        //System.out.println("biofuelPolicies_timeSeries start "+pdObject);
        //System.out.println(pdObject);
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final Map<String, LinkedHashMap<String, String>> map=  pp.exportRestrictionsMeasures_barchart(dsBean, pd_obj);
//        final Map<String, LinkedHashMap<String, String>> map= new HashMap<String, LinkedHashMap<String, String>>();
        //        for( String key: map.keySet())
//        {
//            System.out.println("key "+key);
//            if(key!=null)
//            {
//                Set<String> keySet2= map.get(key).keySet();
//                for(String key2: keySet2)
//                {
//                    System.out.println("time key2 "+key2);
//                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                    Date date = df.parse(key2);
////                    Date date = new Date(key2);
//                    long epoch = date.getTime();
//                    System.out.println(epoch);
//                    System.out.println("country count key "+map.get(key).get(key2));
//                }
//            }
//        }
        /*
        * series: [{
                        name: '1',
                        data: [49, 71, 106]
                    }, {
                        name: '2',
                        data: [93, 106, 84]
                    }]*/

        final JDBCIterablePolicy it = new JDBCIterablePolicy();
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                JSONArray jsonArrayTot = new JSONArray();
                int series_count = 1;
                for( String key: map.keySet())
                {
                    //One obj for each policy type
                    //writer.write("[");
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("name", series_count);
                    JSONArray jsonArray = new JSONArray();
                    Set<String> keySet2= map.get(key).keySet();

                    //Three elements... Ethanol, Biodisel, Biofuel
                    for(String key2: keySet2) {
                        jsonArray.add(Integer.parseInt(map.get(key).get(key2)));
                    }

                    jsonobj.put("data", jsonArray);
                    // writer.write("]");
//                    writer.write("]");
                    jsonArrayTot.add(jsonobj);
                    series_count++;
                }
                writer.write(g.toJson(jsonArrayTot));
                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

        @POST
        @Produces(MediaType.APPLICATION_JSON)
        @Path("/exportSubsidiesPolicyElementLineChart")
        public Response exportSubsidiesPolicyElementLineChart(@FormParam("pdObj") String pdObject) throws Exception {

            POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);

            DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
            String policy_type = "";
            if (pd_obj.getPolicy_type_code() != null)
            {
                for(int i=0; i< pd_obj.getPolicy_type_code().length; i++)
                {
                    policy_type+=pd_obj.getPolicy_type_code()[i];
                    if(i<pd_obj.getPolicy_type_code().length-1)
                    {
                        policy_type+=",";
                    }
                }
            }

            String policy_measure = "";
            if (pd_obj.getPolicy_measure_code() != null)
            {
                for(int i=0; i< pd_obj.getPolicy_measure_code().length; i++)
                {
                    policy_measure+=pd_obj.getPolicy_measure_code()[i];
                    if(i<pd_obj.getPolicy_measure_code().length-1)
                    {
                        policy_measure+=",";
                    }
                }
            }

            final JDBCIterablePolicy it =  pp.getDistinctcpl_id(dsBean, pd_obj, policy_type, policy_measure);

            String cpl_id = "";
            while(it.hasNext()) {
                //[2, Domestic, 1, Agricultural]
                String val = it.next().toString();
                int index = val.lastIndexOf(']');
                val = val.substring(1, index);
                cpl_id +=val;
                cpl_id +=",";
//                val = val.replaceAll("\\s+", "");
            }

            if((cpl_id==null)||(cpl_id.length()==0))
            {
                //  System.out.println("(s.length()==0)");
                //It is not a share group
                // Initiate the stream
                StreamingOutput stream = new StreamingOutput() {

                    @Override
                    public void write(OutputStream os) throws IOException, WebApplicationException {

                        // compute result
                        Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                        // write the result of the query
                        writer.write("[");
                        writer.write("\"NOT_FOUND\"");
                        writer.write("]");
                        // System.out.println("writer "+writer.toString());
                        // Convert and write the output on the stream
                        writer.flush();
                    }

                };
                // Stream result
                return Response.status(200).entity(stream).build();
            }
            else
            {
                cpl_id = cpl_id.substring(0, cpl_id.length()-1);
                final Map<String, LinkedHashMap<String, String>> map=  pp.exportSubsidiesPolicyMeasures_timeSeries(dsBean, pd_obj, cpl_id);
                if((map==null)||(map.size()==0))
                {
                    //  System.out.println("(s.length()==0)");
                    //It is not a share group
                    // Initiate the stream
                    StreamingOutput stream = new StreamingOutput() {

                        @Override
                        public void write(OutputStream os) throws IOException, WebApplicationException {

                            // compute result
                            Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                            // write the result of the query
                            writer.write("[");
                            writer.write("\"NOT_FOUND\"");
                            writer.write("]");
                            // System.out.println("writer "+writer.toString());
                            // Convert and write the output on the stream
                            writer.flush();
                        }

                    };
                    // Stream result
                    return Response.status(200).entity(stream).build();
                }
                else
                {
                    StreamingOutput stream = new StreamingOutput() {

                        @Override
                        public void write(OutputStream os) throws IOException, WebApplicationException {

                            // compute result
                            Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                            JSONArray jsonArrayTot = new JSONArray();
                            int series_count = 1;
                            for( String key: map.keySet())
                            {
                                //One obj for each policy type
                                //writer.write("[");
                                JSONObject jsonobj = new JSONObject();
                                jsonobj.put("name", series_count);
                                JSONArray jsonArray = new JSONArray();
                                Set<String> keySet2= map.get(key).keySet();

                                //Three elements... Ethanol, Biodisel, Biofuel
                                for(String key2: keySet2) {
                                    String valueD = map.get(key).get(key2);
                                    if((valueD!=null)&&(valueD.length()>0))
                                    {
                                        jsonArray.add(Double.parseDouble(valueD));
                                    }
                                    else{
                                        jsonArray.add(null);
                                    }
                                }

                                jsonobj.put("data", jsonArray);
                                jsonArrayTot.add(jsonobj);
                                series_count++;
                            }
                            writer.write(g.toJson(jsonArrayTot));
                            // Convert and write the output on the stream
                            writer.flush();
                        }
                    };
                    // Stream result
                    return Response.status(200).entity(stream).build();
                }
            }
    }

    //Highchart Chart
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/importTariffsPolicyMeasuresBarChart")
    public Response importTariffsPolicyMeasures_barchart(@FormParam("pdObj") String pdObject) throws Exception {
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final Map<String, LinkedHashMap<String, String>> map=  pp.importTariffs_barchart(dsBean, pd_obj);
//        final Map<String, LinkedHashMap<String, String>> map= new HashMap<String, LinkedHashMap<String, String>>();
        //        for( String key: map.keySet())
//        {
//            System.out.println("key "+key);
//            if(key!=null)
//            {
//                Set<String> keySet2= map.get(key).keySet();
//                for(String key2: keySet2)
//                {
//                    System.out.println("time key2 "+key2);
//                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                    Date date = df.parse(key2);
////                    Date date = new Date(key2);
//                    long epoch = date.getTime();
//                    System.out.println(epoch);
//                    System.out.println("country count key "+map.get(key).get(key2));
//                }
//            }
//        }
        /*
        * series: [{
                        name: '1',
                        data: [49, 71, 106]
                    }, {
                        name: '2',
                        data: [93, 106, 84]
                    }]*/

        final JDBCIterablePolicy it = new JDBCIterablePolicy();
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                JSONArray jsonArrayTot = new JSONArray();
                int series_count = 1;
                for( String key: map.keySet())
                {
                    //One obj for each policy type
                    //writer.write("[");
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("name", series_count);
                    JSONArray jsonArray = new JSONArray();
                    JSONArray jsonArrayObservation = new JSONArray();
                    Set<String> keySet2= map.get(key).keySet();
                    JSONObject jsonobjData = new JSONObject();
                    for(String key2: keySet2) {
                        String percentagePlusObs = map.get(key).get(key2);
                        String percentage = "";
                        if((percentagePlusObs!=null)&&(percentagePlusObs.length()>1))
                        {
                            int index = percentagePlusObs.indexOf("%");
                            percentage = percentagePlusObs.substring(0, index);
                            jsonobjData = new JSONObject();
                            jsonobjData.put("y", Double.parseDouble(percentage));
                            jsonobjData.put("name", percentagePlusObs.substring(index+1));
                        }
                        else{
                            //jsonArray.add(null);
                            jsonobjData = null;
                            jsonArrayObservation.add(null);
                        }
                        jsonArray.add(jsonobjData);
                    }

                    //jsonobj.put("data", jsonArray);
                    jsonobj.put("data", jsonArray);
                    jsonobj.put("obs", jsonArrayObservation);
                    // writer.write("]");
//                    writer.write("]");
                    jsonArrayTot.add(jsonobj);
                    series_count++;
                }
                writer.write(g.toJson(jsonArrayTot));
                // Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    //Policy at a Glance Functions End

    //Query and Download Functions Start
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/cplid/{datasource}")
    public Response getCplid(@PathParam("datasource") String datasource) throws Exception {

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getcpl_id(dsBean);

//        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
//        final JDBCIterable it = fp.getODADonors(dsBean, lang);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();

            }

        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/commodityPolicyDomain/{datasource}")
    public Response getpolicyAndcommodityDomain(@PathParam("datasource") String datasource) throws Exception {
        // System.out.println("getpolicyAndcommodityDomain start");
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getpolicyAndcommodityDomain(dsBean);
        //[2, Domestic, 1, Agricultural]

//        while(it.hasNext()) {
//            //[2006-01-01, India]
//            //System.out.println(it.next());
//            String time_country = it.next().toString();
//            String time = time_country.substring(1, time_country.indexOf(','));
//            // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time );
//            if(countryCount_map.containsKey(time))

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JSONArray jsonArray = new JSONArray();
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                // writer.write("[");
                while(it.hasNext()) {
                    //[2, Domestic, 1, Agricultural]
                    String val = it.next().toString();
                    int index= val.lastIndexOf(']');
                    val = val.substring(1,index);
                    val = val.replaceAll("\\s+","");
                    String array[]= val.split(",");
                    JSONObject jsonObj = new JSONObject();
                    jsonObj.put("policy_domain_code", array[0]);
                    jsonObj.put("policy_domain_name", array[1]);
                    jsonObj.put("commoditydomain_code", array[2]);
                    jsonObj.put("commoditydomain_name", array[3]);
                    //  System.out.println("getpolicyAndcommodityDomain "+val);
                    //writer.write(g.toJson(val));
                    jsonArray.add(jsonObj);
//                    if (it.hasNext())
//                        writer.write(",");
                }
                // writer.write("]");
                writer.write(g.toJson(jsonArray));
                // Convert and write the output on the stream
                writer.flush();

                // compute result
//                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
//
//                // write the result of the query
//                writer.write("[");
//                while(it.hasNext()) {
//                    String val = it.next().toString();
//                    System.out.println("getpolicyAndcommodityDomain "+val);
//                    writer.write(g.toJson(val));
//
//                    if (it.hasNext())
//                        writer.write(",");
//                }
//                writer.write("]");
//
//                // Convert and write the output on the stream
//                writer.flush();
            }
        };

        // Stream result
        return Response.status(200).entity(stream).build();
    }
//
//    @GET
//    @Produces(MediaType.APPLICATION_JSON)
//    @Path("/startEndDate/{datasource}")
//    public Response getstartAndEndDate(@PathParam("datasource") String datasource) throws Exception {
//
//        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
//        final JDBCIterable it =  pp.getstartAndEndDate(dsBean);
//
//        // Initiate the stream
//        StreamingOutput stream = new StreamingOutput() {
//
//            @Override
//            public void write(OutputStream os) throws IOException, WebApplicationException {
//
//                // compute result
//                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
//
//                // write the result of the query
//                writer.write("[");
//                while(it.hasNext()) {
//                    // String val = it.next();
//                    //  System.out.println(it.next());
//                    writer.write(g.toJson(it.next()));
//
//                    if (it.hasNext())
//                        writer.write(",");
//                }
//                writer.write("]");
//
//                // Convert and write the output on the stream
//                writer.flush();
//            }
//        };
//
//        // Stream result
//        return Response.status(200).entity(stream).build();
//    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/startEndDate/{datasource}")
    public Response getstartAndEndDate(@PathParam("datasource") String datasource) throws Exception {
        //  System.out.println("getstartAndEndDate start ");
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it = pp.getEndDateIsNull(dsBean);
        // final JDBCIterable it =  pp.getstartAndEndDate(dsBean);
        boolean end_date_null = false;
        while(it.hasNext()) {
            it.next();
            end_date_null = true;
        }
        // System.out.println("end_date_null "+end_date_null);
        final boolean t = end_date_null;
        final JDBCIterablePolicy it2 =  pp.getstartAndEndDate(dsBean);
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
//                writer.write("[");
//                writer.write(g.toJson(t));
//                writer.write("],");

                // write the result of the query
                writer.write("[");
                writer.write("["+g.toJson(t)+"],");
                //  System.out.println("g.toJson(t) "+g.toJson(t));
//                //System.out.println("writer "+writer);
//                writer.write(",");
                while(it2.hasNext()) {
                    // String val = it.next();
                    //   System.out.println("it2 "+it2.next());
                    writer.write(g.toJson(it2.next()));

                    if (it2.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();
            }
        };

        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/policyTypes/{datasource}/{policyDomainCodes}/{commodityDomainCodes}")
    public Response getPolicyTypes(@PathParam("datasource") String datasource, @PathParam("policyDomainCodes") String policyDomainCodes, @PathParam("commodityDomainCodes") String commodityDomainCodes) throws Exception {
        //  System.out.println("getpolicyTypes policyDomainCodes Before  "+policyDomainCodes);
        //  System.out.println("getpolicyTypes commodityDomainCodes Before "+commodityDomainCodes);
        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);
        // System.out.println("getPolicyTypes policyDomainCodes "+policyDomainCodes+" commodityDomainCodes "+commodityDomainCodes);
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getpolicyTypes(dsBean, policyDomainCodes, commodityDomainCodes);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();

            }

        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/downloadPreview")
    public Response getDownloadPreview(@FormParam("pdObj") String pdObject) throws Exception {
        //  System.out.println(" getDownloadPreview start");
        //Gson g = new Gson();
        //  System.out.println(" getDownloadPreview after gson");
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
//        System.out.println(" getDownloadPreview after class");
//        System.out.println(" pd_obj datasource "+pd_obj.getDatasource());
//        System.out.println(" pd_obj policy_domain_code "+pd_obj.getPolicy_domain_code());
//        System.out.println(" pd_obj commodity_domain_code "+pd_obj.getCommodity_domain_code());
//        System.out.println(" pd_obj commodity_class_code "+pd_obj.getCommodity_class_code());
//        System.out.println(" pd_obj policy_type_code "+pd_obj.getPolicy_type_code());
//        for(int i=0; i<pd_obj.getPolicy_type_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_type_code i "+i+" ="+pd_obj.getPolicy_type_code()[i]);
//        }
//        System.out.println(" pd_obj policy_measure_code "+pd_obj.getPolicy_measure_code());
//        for(int i=0; i<pd_obj.getPolicy_measure_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_measure_code i "+i+" ="+pd_obj.getPolicy_measure_code()[i]);
//        }
//        System.out.println(" pd_obj country_code "+pd_obj.getCountry_code());
//        System.out.println(" pd_obj yearTab "+pd_obj.getYearTab());
//        System.out.println(" pd_obj year_list "+pd_obj.getYear_list());
//        System.out.println(" pd_obj start_date "+pd_obj.getStart_date());
//        System.out.println(" pd_obj end_date "+pd_obj.getEnd_date());
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);

//        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
//        Gson g = new Gson();
        //g.toJsonTree(qvo);
        //System.out.println("FIRST ");
        // AMISQueryVO vo = g.fromJson(qvo, AMISQueryVO.class);
        boolean with_commodity_id = false;
        final JDBCIterablePolicy it =  pp.getDownloadPreview(dsBean, pd_obj, with_commodity_id);
        // System.out.println("after pp.getDownloadPreview");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    // System.out.println("in loop next");
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();

            }

        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Path("/streamexcel")
    //'AllData'= Master+Policy tables
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
        //  System.out.println("streamExcel start ");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                //  Gson g = new Gson();
//                SQLBean sql = g.fromJson(json, SQLBean.class);
//                DatasourceBean db = datasourcePool.getDatasource(datasource.toUpperCase());
                   System.out.println("streamExcel 1 ");
                POLICYDataObject pd_obj = g.fromJson(json, POLICYDataObject.class);
                System.out.println("streamExcel 2 ");
                System.out.println(" pd_obj datasource "+pd_obj.getDatasource());
                System.out.println(" pd_obj policy_domain_code "+pd_obj.getPolicy_domain_code());
                System.out.println(" pd_obj commodity_domain_code "+pd_obj.getCommodity_domain_code());
                System.out.println(" pd_obj commodity_class_code "+pd_obj.getCommodity_class_code());
                System.out.println(" pd_obj policy_type_code "+pd_obj.getPolicy_type_code());
                for(int i=0; i<pd_obj.getPolicy_type_code().length;i++)
                {
                    System.out.println(" pd_obj policy_type_code i "+i+" ="+pd_obj.getPolicy_type_code()[i]);
                }
                System.out.println(" pd_obj policy_measure_code len  "+pd_obj.getPolicy_measure_code().length);
                for(int i=0; i<pd_obj.getPolicy_measure_code().length;i++)
                {
                    System.out.println(" pd_obj policy_measure_code i "+i+" ="+pd_obj.getPolicy_measure_code()[i]);
                }
                System.out.println(" pd_obj country_code "+pd_obj.getCountry_code());
                System.out.println(" pd_obj yearTab "+pd_obj.getYearTab());
                System.out.println(" pd_obj year_list "+pd_obj.getYear_list());
                System.out.println(" pd_obj start_date "+pd_obj.getStart_date());
                System.out.println(" pd_obj end_date "+pd_obj.getEnd_date());

                DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // compute result
                JDBCIterablePolicy it = new JDBCIterablePolicy();

                try {
                    //Master + Policy
                    it =  pp.getDownloadExport(dsBean, pd_obj);

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

//                String[] headerArray = {"metadata_id","policy_id","cpl_id","commodity_id","hs_version","hs_code","hs_suffix","commodity_description","shared_group_code","location_condition","start_date","end_date","units","value","value_text","exemptions","value_calculated","notes","link","source","title_of_notice","legal_basis_name","measure_descr","short_description","original_dataset","type_of_change_name","type_of_change_code","product_original_hs","product_original_name","policy_element","impl","second_generation_specific","imposed_end_date", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "source_benchmark", "date_of_publication", "xs_yeartype", "notes_datepub", "country_code", "country_name", "subnational_code", "subnational_name", "commoditydomain_code", "commoditydomain_name", "commodityclass_code", "commodityclass_name", "policydomain_code", "policydomain_name", "policytype_code", "policytype_name", "policymeasure_code", "policymeasure_name", "condition"};
                //String[] headerArray = {"metadata_id","policy_id","cpl_id","country_code", "country_name", "subnational_code", "subnational_name", "commoditydomain_code", "commoditydomain_name", "commodityclass_code", "commodityclass_name", "policydomain_code", "policydomain_name", "policytype_code", "policytype_name", "policymeasure_code", "policymeasure_name", "condition","commodity_id","hs_version","hs_code","hs_suffix","commodity_description","shared_group_code","location_condition","start_date","end_date","units","value","value_text","exemptions","value_calculated","notes","link","source","title_of_notice","legal_basis_name","measure_descr","short_description","original_dataset","type_of_change_name","type_of_change_code","product_original_hs","product_original_name","policy_element","impl","second_generation_specific","imposed_end_date", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "source_benchmark", "date_of_publication", "xs_yeartype", "notes_datepub"};

//                String[] headerArray = {"metadata_id","policy_id","cpl_id","cpl_code","country_code", "country_name", "subnational_code", "subnational_name", "commoditydomain_code", "commoditydomain_name", "commodityclass_code", "commodityclass_name", "policydomain_code", "policydomain_name", "policytype_code", "policytype_name", "policymeasure_code", "policymeasure_name", "condition_code", "condition", "individualpolicy_code", "individualpolicy_name", "commodity_id","hs_version","hs_code","hs_suffix", "short_description", "shared_group_code", "policy_element", "start_date","end_date", "units","value","value_text", "value_type", "exemptions", "location_condition", "notes", "link","source","title_of_notice", "legal_basis_name", "date_of_publication", "imposed_end_date", "second_generation_specific", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "benchmark_link", "original_dataset", "type_of_change_code", "type_of_change_name", "measure_descr", "product_original_hs","product_original_name", "implementationprocedure","xs_yeartype", "link_pdf", "benchmark_link_pdf"};
                //Before removing policy_id
//                String[] headerArray = {"policy_id","cpl_id","cpl_code","country_code", "country_name", "subnational_code", "subnational_name", "commoditydomain_code", "commoditydomain_name", "commodityclass_code", "commodityclass_name", "policydomain_code", "policydomain_name", "policytype_code", "policytype_name", "policymeasure_code", "policymeasure_name", "condition_code", "condition", "individualpolicy_code", "individualpolicy_name", "commodity_id","hs_version","hs_code","hs_suffix", "short_description", "shared_group_code", "policy_element", "start_date","end_date", "units","value","value_text", "value_type", "exemptions", "location_condition", "notes", "link","source","title_of_notice", "legal_basis_name", "date_of_publication", "imposed_end_date", "second_generation_specific", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "benchmark_link", "original_dataset", "type_of_change_code", "type_of_change_name", "measure_descr", "product_original_hs","product_original_name", "implementationprocedure","xs_yeartype", "link_pdf", "benchmark_link_pdf"};
                //After removing policy_id
                //String[] headerArray = {"cpl_id","cpl_code","country_code", "country_name", "subnational_code", "subnational_name", "commoditydomain_code", "commoditydomain_name", "commodityclass_code", "commodityclass_name", "policydomain_code", "policydomain_name", "policytype_code", "policytype_name", "policymeasure_code", "policymeasure_name", "condition_code", "condition", "individualpolicy_code", "individualpolicy_name", "commodity_id","hs_version","hs_code","hs_suffix", "short_description", "shared_group_code", "policy_element", "start_date","end_date", "units","value","value_text", "value_type", "exemptions", "location_condition", "notes", "link","source","title_of_notice", "legal_basis_name", "date_of_publication", "imposed_end_date", "second_generation_specific", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "benchmark_link", "original_dataset", "type_of_change_code", "type_of_change_name", "measure_descr", "product_original_hs","product_original_name", "implementationprocedure","xs_yeartype", "link_pdf", "benchmark_link_pdf"};
                //New order
                String[] headerArray = {"policy_id","cpl_id", "country_name", "subnational_name", "commoditydomain_name", "commodityclass_name", "policydomain_name", "policytype_name", "policymeasure_name", "condition", "individualpolicy_name", "commodity_id","hs_version","hs_code","hs_suffix", "short_description", "description", "shared_group_code", "policy_element", "start_date","end_date", "units","value","value_text", "value_type", "exemptions", "location_condition", "notes", "link","source","title_of_notice", "legal_basis_name", "date_of_publication", "imposed_end_date", "second_generation_specific", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "benchmark_link", "original_dataset", "type_of_change_name", "measure_description", "product_original_hs","product_original_name", "link_pdf", "benchmark_link_pdf","country_code","subnational_code","commoditydomain_code","commodityclass_code","policydomain_code","policytype_code","policymeasure_code","condition_code","individualpolicy_code","type_of_change_code"};
                //With cpl_code start
//                String[] headerArray = {"policy_id","cpl_id", "country_name", "subnational_name", "commoditydomain_name", "commodityclass_name", "policydomain_name", "policytype_name", "policymeasure_name", "condition", "individualpolicy_name", "commodity_id","hs_version","hs_code","hs_suffix", "short_description", "description", "shared_group_code", "policy_element", "start_date","end_date", "units","value","value_text", "value_type", "exemptions", "location_condition", "notes", "link","source","title_of_notice", "legal_basis_name", "date_of_publication", "imposed_end_date", "second_generation_specific", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "benchmark_link", "original_dataset", "type_of_change_name", "measure_descr", "product_original_hs","product_original_name", "implementationprocedure","xs_yeartype", "link_pdf", "benchmark_link_pdf","cpl_code","country_code","subnational_code","commoditydomain_code","commodityclass_code","policydomain_code","policytype_code","policymeasure_code","condition_code","individualpolicy_code","type_of_change_code"};
                //With cpl_code end

                // write the result of the query
                writer.write("<html><head><meta charset=\"UTF-8\"></head><body>");
//                <html>
//                <head>
//                <meta charset="UTF-8">
//                </head>
//                <body>
//                </body>
//                </html>
                DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                Date date = new Date();
                //System.out.println(dateFormat.format(date));

                writer.write("<table border=\"1\">");
                // System.out.println("Table");
                writer.write("<tr>");
                writer.write("<td><b>");
                writer.write("Date:");
                writer.write("</b></td>");
                writer.write("<td>");
                writer.write(dateFormat.format(date));
                writer.write("</td>");
                writer.write("</tr>");

                writer.write("<tr>");
                writer.write("<td><b>");
                writer.write("Source:");
                writer.write("</b></td>");
                writer.write("<td>");
                writer.write("AMIS Policy Database");
                writer.write("</td>");
                writer.write("</tr>");

                writer.write("<tr>");
                writer.write("</tr>");

                writer.write("<tr>");
                for (int i = 0; i < headerArray.length; i++) {
                    writer.write("<td><b>");
                    writer.write(headerArray[i]);
                    writer.write("</b></td>");
                }
                writer.write("</tr>");
                //   System.out.println("it.hasNext() "+it.hasNext());
                while(it.hasNext()) {

                    List<String> l = it.next();
                    // System.out.println("loop it.hasNext() "+it.hasNext()+" "+l.size());
                    writer.write("<tr>");
                    for (int i = 0; i < l.size(); i++) {
                        writer.write("<td nowrap>");
//                        if((i==15)||(i==16))
//                        {
//                            //Short Description and Description  .... no wrap
//                            writer.write("<td nowrap>");
//                        }
//                        else{
//                            writer.write("<td>");
//                        }
                        //writer.write("<td>");
                        if((l==null)||(l.isEmpty())||(l.get(i)==null))
                        {
                            writer.write("");
                        }
                        else
                        {
                            writer.write(l.get(i));
                        }
                        writer.write("</td>");
                    }
                    writer.write("</tr>");
                }
                writer.write("</table>");
                writer.write("</body></html>");
                //System.out.println("Before writer");
                // System.out.println(writer.toString());
                // Convert and write the output on the stream
                writer.flush();
                writer.close();
            }

        };

        // Wrap result
        Response.ResponseBuilder builder = Response.ok(stream);
//        builder.header("Access-Control-Allow-Origin", "*");
//        builder.header("Access-Control-Max-Age", "3600");
//        builder.header("Access-Control-Allow-Methods", "POST");
//        builder.header("Access-Control-Allow-Headers", "X-Requested-With, Host, User-Agent, Accept, Accept-Language, Accept-Encoding, Accept-Charset, Keep-Alive, Connection, Referer,Origin");
//        builder.header("Content-Disposition", "attachment; filename=" + UUID.randomUUID().toString() + ".xls");

        builder.header("Content-Disposition", "attachment; filename= PolicyData_" + UUID.randomUUID().toString() + ".xls");
        builder.header("Content-type",  "application/vnd.ms-excel; charset=UTF-8");
        // Stream Excel
        //System.out.println("Before build");
        return builder.build();

    }

    @POST
    @Path("/streamexcel2")
    // 'ShareGroupData' = Share Group information
    public Response streamExcel2(final @FormParam("datasource_WQ2") String datasource,
                                 final @FormParam("json_WQ2") String json,
                                 final @FormParam("cssFilename_WQ2") String cssFilename,
                                 final @FormParam("valueIndex_WQ2") String valueIndex,
                                 final @FormParam("thousandSeparator_WQ2") String thousandSeparator,
                                 final @FormParam("decimalSeparator_WQ2") String decimalSeparator,
                                 final @FormParam("decimalNumbers_WQ2") String decimalNumbers,
                                 final @FormParam("quote_WQ2") String quote,
                                 final @FormParam("title_WQ2") String title,
                                 final @FormParam("subtitle_WQ2") String subtitle) {
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                POLICYDataObject pd_obj = g.fromJson(json, POLICYDataObject.class);

                DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // compute result
                JDBCIterablePolicy it = new JDBCIterablePolicy();
                Map<String, Map<String, LinkedList<String>>> commodity_info = new  LinkedHashMap<String, Map<String, LinkedList<String>>>();
                try {
                    //Share Group
                    commodity_info =  pp.getDownloadShareGroupExport(dsBean, pd_obj);

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

//                String[] headerArray = {"metadata_id","policy_id","cpl_id","commodity_id","hs_version","hs_code","hs_suffix","commodity_description","shared_group_code","location_condition","start_date","end_date","units","value","value_text","exemptions","value_calculated","notes","link","source","title_of_notice","legal_basis_name","measure_descr","short_description","original_dataset","type_of_change_name","type_of_change_code","product_original_hs","product_original_name","policy_element","impl","second_generation_specific","imposed_end_date", "benchmark_tax", "benchmark_product", "tax_rate_biofuel", "tax_rate_benchmark", "start_date_tax", "source_benchmark", "date_of_publication", "xs_yeartype", "notes_datepub", "country_code", "country_name", "subnational_code", "subnational_name", "commoditydomain_code", "commoditydomain_name", "commodityclass_code", "commodityclass_name", "policydomain_code", "policydomain_name", "policytype_code", "policytype_name", "policymeasure_code", "policymeasure_name", "condition"};
                // String[] headerArray = {"commodity_id","target_code","description"};
//                String[] headerArray = {"hs_code", "hs_suffix", "hs_version", "short_description", "commodity_id", "shared_group_code"};
                String[] headerArray = {"shared_group_commodity_id", "shared_group_code", "commodity_id", "hs_code", "hs_suffix", "hs_version", "short_description", "original_hs_version", "original_hs_code", "original_hs_suffix"};
                // write the result of the query
                writer.write("<html><head><meta charset=\"UTF-8\"></head><body>");
                writer.write("<table border=\"1\">");

                writer.write("<tr>");
                for (int i = 0; i < headerArray.length; i++) {
                    writer.write("<td>");
                    writer.write(headerArray[i]);
                    writer.write("</td>");
                }
                writer.write("</tr>");
                //All the share group
                Set<String> keySet = commodity_info.keySet();
                for(String key: keySet)
                {
                    //For each share group
                    Set<String> keySet2 = commodity_info.get(key).keySet();
                    for(String key2: keySet2)
                    {
                        //For each commodity of each share group
                        LinkedList<String> l = commodity_info.get(key).get(key2);
                        writer.write("<tr>");
                        //Share group ... commodity id
                        writer.write("<td>");
                        if((key==null)||(key.isEmpty()))
                        {
                            writer.write("");
                        }
                        else
                        {
                            writer.write(key);
                        }
                        writer.write("</td>");
                        //Share group ... code
                        writer.write("<td>");
                        if((l==null)||(l.isEmpty())||(l.get(0)==null))
                        {
                            writer.write("");
                        }
                        else
                        {
                            writer.write(l.get(0));
                        }
                        writer.write("</td>");
                        //Commodity ... commodity id
                        writer.write("<td>");
                        if((key2==null)||(key2.isEmpty()))
                        {
                            writer.write("");
                        }
                        else
                        {
                            writer.write(key2);
                        }
                        writer.write("</td>");
                        for (int i = 1; i < l.size(); i++) {
                            writer.write("<td>");
                            if((l==null)||(l.isEmpty())||(l.get(i)==null))
                            {
                                writer.write("");
                            }
                            else
                            {
                                writer.write(l.get(i));
                            }
                            writer.write("</td>");
                        }
                        writer.write("</tr>");
                    }
                }
//                for(String key: keySet)
//                {
//                    LinkedList<String> l = commodity_info.get(key);
//                    writer.write("<tr>");
////                    writer.write("<td>");
////                    if(key==null)
////                    {
////                        writer.write("");
////                    }
////                    else
////                    {
////                        writer.write(key);
////                    }
////                    writer.write("</td>");
//                    for (int i = 0; i < l.size(); i++) {
//                        writer.write("<td>");
//                        if((l==null)||(l.isEmpty())||(l.get(i)==null))
//                        {
//                            writer.write("");
//                        }
//                        else
//                        {
//                            writer.write(l.get(i));
//                        }
//                        writer.write("</td>");
//                    }
//                    writer.write("</tr>");
//                }
                writer.write("</table>");
                writer.write("</body></html>");

                // System.out.println(writer.toString());

                // Convert and write the output on the stream
                writer.flush();
                writer.close();
            }

        };

        // Wrap result
        Response.ResponseBuilder builder = Response.ok(stream);
        builder.header("Content-Disposition", "attachment; filename= SharedGroupData_" + UUID.randomUUID().toString() + ".xls");
        builder.header("Content-type",  "application/vnd.ms-excel; charset=UTF-8");

        // Stream Excel
        return builder.build();

    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/masterFromCplId")
    public Response getMasterFromCplId(@FormParam("pdObj") String pdObject) throws Exception {
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final JDBCIterablePolicy it =  pp.getMasterFromCplId(dsBean, pd_obj);
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                writer.write("[");
                while(it.hasNext()) {
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();
            }
        };

        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/masterFromCplIdAndSubnational")
    public Response getMasterFromCplIdAndSubnational(@FormParam("pdObj") String pdObject, @FormParam("map") String map2, @FormParam("map2") String map4, @FormParam("map3") String map5,@FormParam("map4") String map6) throws Exception {

        LinkedHashMap<String,String> map=new LinkedHashMap<String,String>();
        map=(LinkedHashMap<String,String>) g.fromJson(map2, map.getClass());

        LinkedHashMap<String,LinkedHashMap<String,String>> map3=new LinkedHashMap<String,LinkedHashMap<String,String>>();
        map3=(LinkedHashMap<String,LinkedHashMap<String,String>>) g.fromJson(map4, map3.getClass());

        LinkedHashMap<String,String> map7=new LinkedHashMap<String,String>();
        map7=(LinkedHashMap<String,String>) g.fromJson(map5, map7.getClass());

        LinkedHashMap<String,LinkedHashMap<String,String>> map8=new LinkedHashMap<String,LinkedHashMap<String,String>>();
        map8=(LinkedHashMap<String,LinkedHashMap<String,String>>) g.fromJson(map6, map8.getClass());

        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        pd_obj.setSubnationalMap(map);
        pd_obj.setSubnational_for_coutryMap(map3);
        pd_obj.setSubnationalMap_level_3(map7);
        pd_obj.setSubnational_for_coutryMap_level_3(map8);

        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final LinkedList<String[]> list =  pp.getMasterFromCplIdAndSubnational(dsBean, pd_obj);
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                //writer.write("[");
//                while(it.hasNext()) {
//                    writer.write(g.toJson(it.next()));
//                    if (it.hasNext())
//                        writer.write(",");
//                }
//                for(int i =0; i<list.size(); i++){
//                    String stringArray[] = list.get(i);
//                    String sApp ="[";
//                    System.out.println("Before string array");
//                    for(int j=0; j<stringArray.length; j++){
////                        System.out.println(stringArray[j]);
//                        sApp += "\""+stringArray[j]+"\",";
//                    }
//                    if((sApp!=null)&&(sApp.length()>0)){
//                        sApp = sApp.substring(0, sApp.length()-1);
//                        sApp += "]";
//                    }
//                    System.out.println(sApp);
//                    //writer.write(g.toJson(stringArray.toString()));
////                    writer.write(g.toJson(Arrays.toString(stringArray)));
//                    writer.write(g.toJson(sApp));
//                    if (i<list.size()-1)
//                        writer.write(",");
//                }
                    writer.write(g.toJson(list));

               // writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
            }
        };

        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/masterFromCplIdAndNegativeSubnational")
    public Response getMasterFromCplIdAndNegativeSubnational(@FormParam("pdObj") String pdObject) throws Exception {

        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);

        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //The format of cplId is this: 'code1', 'code2', 'code3'
        final LinkedList<String[]> list =  pp.getMasterFromCplIdAndNegativeSubnational(dsBean, pd_obj);
        //  System.out.println("getMasterFromCplId after getMasterFromCplId");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                //writer.write("[");
//                while(it.hasNext()) {
//                    writer.write(g.toJson(it.next()));
//                    if (it.hasNext())
//                        writer.write(",");
//                }
//                for(int i =0; i<list.size(); i++){
//                    String stringArray[] = list.get(i);
//                    String sApp ="[";
//                    System.out.println("Before string array");
//                    for(int j=0; j<stringArray.length; j++){
////                        System.out.println(stringArray[j]);
//                        sApp += "\""+stringArray[j]+"\",";
//                    }
//                    if((sApp!=null)&&(sApp.length()>0)){
//                        sApp = sApp.substring(0, sApp.length()-1);
//                        sApp += "]";
//                    }
//                    System.out.println(sApp);
//                    //writer.write(g.toJson(stringArray.toString()));
////                    writer.write(g.toJson(Arrays.toString(stringArray)));
//                    writer.write(g.toJson(sApp));
//                    if (i<list.size()-1)
//                        writer.write(",");
//                }
                writer.write(g.toJson(list));

                // writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
            }
        };

        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/mapData")
    public Response getMapData(@FormParam("pdObj") String pdObject) throws Exception {

        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        final LinkedHashMap<String, String> countryMap = pp.getCountryMapData(dsBean, pd_obj);
        final LinkedHashMap<String, String> subnationalMap = pp.getSubnationalMapData(dsBean, pd_obj);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                JSONObject jsonObj = new JSONObject();
                jsonObj.put("country_map", countryMap);
                jsonObj.put("subnational_map", subnationalMap);

                writer.write(g.toJson(jsonObj));

                // Convert and write the output on the stream
                writer.flush();
            }
        };

        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/shareGroupInfo/{datasource}/{commodity_id}")
    public Response getShareGroupInfo(@PathParam("datasource") String datasource, @PathParam("commodity_id") String commodityId) throws Exception {
        // System.out.println("getShareGroupInfo cplId Before  "+commodityId);

        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        // commodityId = "169";
        final JDBCIterablePolicy it =  pp.getshareGroupInfo(dsBean, commodityId);
        //final JDBCIterablePolicy it =  pp.getshareGroupInfo(dsBean, "169");

        while(it.hasNext()) {
            //Get the share group code associated to the commodity_id
            // String share_group_code = g.toJson(it.next());
            String valueshare_group_code = g.toJson(it.next());
            String share_group_code = valueshare_group_code.substring(1,(valueshare_group_code.length()-1));
            // System.out.println(" share_group_code *"+share_group_code+"*");
            //  System.out.println(" share_group_code "+share_group_code);
            if((share_group_code!=null)&&(!share_group_code.equals("none"))&&(!share_group_code.equals("\"none\"")))
            {
                //Get the list of commodity that belongs to this share_group_code
                final JDBCIterablePolicy it2 = pp.getSingleIdFromCommodityId(dsBean, commodityId);
                //In the Share Group Table show the info of the share group and the each commodity that belong to the share group
//                String s = ""+commodityId+",";
                String s ="";
                while(it2.hasNext()) {
                    //From [289] to 289
                    List<String> valueList = it2.next();
                    //  System.out.println("valueList "+valueList.toString());
                    //There is only one value
                    String value = valueList.toString();
//                    value = value.substring(2, value.lastIndexOf("\""));
//                   s+= "'"+g.toJson(it2.next())+"'";
//                    s+= "'"+it2.next()+"'";

                    s+= value.substring(1,(value.length()-1));
                    //  System.out.println("value "+value+" new value "+s);
                    if (it2.hasNext())
                        s+=",";
                }
                // System.out.println("After getSingleIdFromCommodityId s = "+s);
                if(s.length()==0)
                {
                    //  System.out.println("(s.length()==0)");
                    //It is not a share group
                    // Initiate the stream
                    StreamingOutput stream = new StreamingOutput() {

                        @Override
                        public void write(OutputStream os) throws IOException, WebApplicationException {

                            // compute result
                            Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                            // write the result of the query
                            writer.write("[");
                            writer.write("\"NOT_FOUND\"");
                            writer.write("]");
                            // System.out.println("writer "+writer.toString());
                            // Convert and write the output on the stream
                            writer.flush();
                        }

                    };
                    // Stream result
                    return Response.status(200).entity(stream).build();
                }
                else
                {
                    //  System.out.println("(s.length()!0)");
                    //Get commodity_id, target_code, description associated with the commodityId
                    final JDBCIterablePolicy it3 = pp.getCommodityInfo(dsBean, s);
                    // Initiate the stream
                    StreamingOutput stream = new StreamingOutput() {

                        @Override
                        public void write(OutputStream os) throws IOException, WebApplicationException {

                            // compute result
                            Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                            // write the result of the query
                            writer.write("[");
                            while(it3.hasNext()) {
                                //Without saving commodity_id
                                // String commodity_id = it3.next().toString();
                                writer.write(g.toJson(it3.next()));
                                if (it3.hasNext())
                                    writer.write(",");
                            }
                            writer.write("]");
                            // Convert and write the output on the stream
                            writer.flush();
                            // Stream result
                        }

                    };
                    //  System.out.println("stream "+stream);
                    return Response.status(200).entity(stream).build();
                }
            }
            else{
                //It is not a share group
                // Initiate the stream
                StreamingOutput stream = new StreamingOutput() {

                    @Override
                    public void write(OutputStream os) throws IOException, WebApplicationException {

                        // compute result
                        Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                        // write the result of the query
                        writer.write("[");
                        writer.write("\"NOT_FOUND\"");
                        writer.write("]");

                        // Convert and write the output on the stream
                        writer.flush();
                    }

                };
                // Stream result
                return Response.status(200).entity(stream).build();
            }
        }
        //It never arrives here
        return Response.status(200).entity("").build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/downloadPreviewPolicyTable")
    public Response getDownloadPreviewPolicyTable(@FormParam("pdObj") String pdObject) throws Exception {
        //  System.out.println(" getDownloadPreviewPolicyTable start");

        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
//        System.out.println(" getDownloadPreviewPolicyTable after class");
//        System.out.println(" pd_obj datasource "+pd_obj.getDatasource());
//        System.out.println(" pd_obj policy_domain_code "+pd_obj.getPolicy_domain_code());
//        System.out.println(" pd_obj commodity_domain_code "+pd_obj.getCommodity_domain_code());
//        System.out.println(" pd_obj commodity_class_code "+pd_obj.getCommodity_class_code());
//        System.out.println(" pd_obj policy_type_code "+pd_obj.getPolicy_type_code());
//        for(int i=0; i<pd_obj.getPolicy_type_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_type_code i "+i+" ="+pd_obj.getPolicy_type_code()[i]);
//        }
//        System.out.println(" pd_obj policy_measure_code "+pd_obj.getPolicy_measure_code());
//        for(int i=0; i<pd_obj.getPolicy_measure_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_measure_code i "+i+" ="+pd_obj.getPolicy_measure_code()[i]);
//        }
//        System.out.println(" pd_obj country_code "+pd_obj.getCountry_code());
//        System.out.println(" pd_obj yearTab "+pd_obj.getYearTab());
//        System.out.println(" pd_obj year_list "+pd_obj.getYear_list());
//        System.out.println(" pd_obj start_date "+pd_obj.getStart_date());
//        System.out.println(" pd_obj end_date "+pd_obj.getEnd_date());
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

        final JDBCIterablePolicy it =  pp.getPolicyFromCplId(dsBean, pd_obj);
        //  System.out.println("after pp.getDownloadPreviewPolicyTable");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                int i=0;
                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //  System.out.println("in loop next i="+i+" elem "+it.next());

                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                    i++;
                }
                writer.write("]");

                //Convert and write the output on the stream
                writer.flush();
            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }
    //Query and Download Functions End

    //Policy Data Entry
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/units/{datasource}")
    public Response getUnits(@PathParam("datasource") String datasource) throws Exception {

        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getUnits(dsBean);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("{\"data\": [");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    String s = g.toJson(it.next());
                    s = s.substring(1, s.length()-1);
                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";
//                    int comma_index = s.indexOf(",");
//                    s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";

                    writer.write(s);
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]}");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    //Policy Data Entry
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/policyElement/{datasource}/{policyMeasure}")
    public Response getPolicyElement(@PathParam("datasource") String datasource, @PathParam("policyMeasure") String policyMeasure) throws Exception {

        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getPolicyElement(dsBean, policyMeasure);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("{\"data\": [");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    String s = g.toJson(it.next());
                    s = s.substring(1, s.length()-1);
                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";
//                    int comma_index = s.indexOf(",");
//                    s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";

                    writer.write(s);
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]}");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/source/{datasource}/{countryCode}/{cplId}")
    public Response getSource(@PathParam("datasource") String datasource, @PathParam("countryCode") String countryCode, @PathParam("cplId") String cplId) throws Exception {

        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);

        System.out.println("getSource countryCode= "+countryCode +" cpl_id= "+cplId);

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getSource(dsBean, countryCode);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("{\"data\": [");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    String s = g.toJson(it.next());
                    s = s.substring(1, s.length()-1);
                    //int comma_index = s.indexOf(",");
                    //System.out.println(s);
                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";

                    //s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";

                    writer.write(s);
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]}");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sourceWithoutWTO/{datasource}/{countryCode}")
    public Response getSourceNotWTO(@PathParam("datasource") String datasource, @PathParam("countryCode") String countryCode) throws Exception {

        // compute result
//        DATASOURCE ds = DATASOURCE.POLICY;
//        DBBean db = new DBBean(ds);

        System.out.println("getSource countryCode= "+countryCode);

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getSourceWithoutWTOSource(dsBean, countryCode);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("{\"data\": [");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    String s = g.toJson(it.next());
                    s = s.substring(1, s.length()-1);
                    //int comma_index = s.indexOf(",");
                    //System.out.println(s);
                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";

                    //s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";

                    writer.write(s);
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]}");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/secondGenerationSpecific/{datasource}")
    public Response getSecondGenerationSpecific(@PathParam("datasource") String datasource) throws Exception {
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getSecondGenerationSpecific(dsBean);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("{\"data\": [");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    String s = g.toJson(it.next());
                    s = s.substring(1, s.length()-1);
                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";
//                    int comma_index = s.indexOf(",");
//                    s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";

                    writer.write(s);
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]}");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/imposedEndDate/{datasource}")
    public Response getImposedEndDate(@PathParam("datasource") String datasource) throws Exception {
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getImposedEndDate(dsBean);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("{\"data\": [");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    String s = g.toJson(it.next());
                    s = s.substring(1, s.length()-1);
                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";
//                    int comma_index = s.indexOf(",");
//                    s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";

                    writer.write(s);
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]}");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/localCondition/{datasource}")
    public Response getLocalCondition(@PathParam("datasource") String datasource) throws Exception {

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getLocalCondition(dsBean);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("{\"data\": [");
                while(it.hasNext()) {
                    // System.out.println(it.next());
                    String s = g.toJson(it.next());
                    s = s.substring(1, s.length()-1);
//                    System.out.println(s);
//                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";
                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";
//                    int comma_index = s.indexOf(",");
//                    s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";

                    writer.write(s);
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]}");

                // Convert and write the output on the stream
                writer.flush();

            }
        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/savePolicyDataEntry")
//    public Response savePolicy_dataEditor(@FormParam("pdObj") String pdObject) throws Exception {
    public Response savePolicy_dataEditor(String pdObject) throws Exception {
          System.out.println(" savePolicy_dataEditor start");

        System.out.println(pdObject);

 //       POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
//        System.out.println(" getDownloadPreviewPolicyTable after class");
//        System.out.println(" pd_obj datasource "+pd_obj.getDatasource());
//        System.out.println(" pd_obj policy_domain_code "+pd_obj.getPolicy_domain_code());
//        System.out.println(" pd_obj commodity_domain_code "+pd_obj.getCommodity_domain_code());
//        System.out.println(" pd_obj commodity_class_code "+pd_obj.getCommodity_class_code());
//        System.out.println(" pd_obj policy_type_code "+pd_obj.getPolicy_type_code());
//        for(int i=0; i<pd_obj.getPolicy_type_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_type_code i "+i+" ="+pd_obj.getPolicy_type_code()[i]);
//        }
//        System.out.println(" pd_obj policy_measure_code "+pd_obj.getPolicy_measure_code());
//        for(int i=0; i<pd_obj.getPolicy_measure_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_measure_code i "+i+" ="+pd_obj.getPolicy_measure_code()[i]);
//        }
//        System.out.println(" pd_obj country_code "+pd_obj.getCountry_code());
//        System.out.println(" pd_obj yearTab "+pd_obj.getYearTab());
//        System.out.println(" pd_obj year_list "+pd_obj.getYear_list());
//        System.out.println(" pd_obj start_date "+pd_obj.getStart_date());
//        System.out.println(" pd_obj end_date "+pd_obj.getEnd_date());
  //      DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

   //     final JDBCIterablePolicy it =  pp.getPolicyFromCplId(dsBean, pd_obj);
        //  System.out.println("after pp.getDownloadPreviewPolicyTable");
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                int i=0;
                // write the result of the query
                writer.write("[");
//                while(it.hasNext()) {
//                    //  System.out.println("in loop next i="+i+" elem "+it.next());
//
//                    writer.write(g.toJson(it.next()));
//                    if (it.hasNext())
//                        writer.write(",");
//                    i++;
//                }
                writer.write("]");

                //Convert and write the output on the stream
                writer.flush();
            }
        };

        //return Response.status(200).entity(stream).build();
        return Response.status(200).entity(null).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)//selecteditem_commodityDomain.code+ '/' + selecteditem_policyDomain.code+ '/'+commodityClassCode;
    @Path("/commodity/{datasource}/{commodityDomainCode}/{policyDomainCode}/{commodityClassCode}/{countryCode}/{withSharedGroups}")
    //public Response getCommodity(@PathParam("datasource") String datasource, @PathParam("commodityClassCode") String commodityClassCode, @PathParam("countryCode") String countryCode) throws Exception {
    public Response getCommodity(@PathParam("datasource") String datasource, @PathParam("commodityDomainCode") String commodityDomainCode, @PathParam("policyDomainCode") String policyDomainCode, @PathParam("commodityClassCode") String commodityClassCode, @PathParam("countryCode") String countryCode, @PathParam("withSharedGroups") String withSharedGroups) throws Exception {

        System.out.println("getCommodity start");
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        Boolean withSharedGroupsBool = false;
        if(withSharedGroups.equalsIgnoreCase("true")){
            withSharedGroupsBool = true;
        }
//        final JDBCIterablePolicy it =  pp.getCommodityInfoByCommodityClassCode(dsBean, commodityClassCode, countryCode, policyMeasureCode);
        final JDBCIterablePolicy it =  pp.getCommodityInfoWithUnionHS4HS6(dsBean, commodityDomainCode, policyDomainCode, commodityClassCode, countryCode, withSharedGroupsBool);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)//selecteditem_commodityDomain.code+ '/' + selecteditem_policyDomain.code+ '/'+commodityClassCode;
    @Path("/commodityIgnoringAssociatedPolicy/{datasource}/{commodityClassCode}/{countryCode}/{withSharedGroups}")
    public Response getCommodityIgnoringAssociatedPolicy(@PathParam("datasource") String datasource, @PathParam("commodityClassCode") String commodityClassCode, @PathParam("countryCode") String countryCode, @PathParam("withSharedGroups") String withSharedGroups) throws Exception {

        System.out.println("getCommodity start");
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        Boolean withSharedGroupsBool = false;
        if(withSharedGroups.equalsIgnoreCase("true")){
            withSharedGroupsBool = true;
        }
//        final JDBCIterablePolicy it =  pp.getCommodityInfoByCommodityClassCode(dsBean, commodityClassCode, countryCode, policyMeasureCode);
        final JDBCIterablePolicy it =  pp.getCommodityInfoIgnoringAssociatedPolicy(dsBean, commodityClassCode, countryCode, withSharedGroupsBool);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/commodityByClass/{datasource}/{countryCode}/{commodityClassCode}")
    public Response getCommodityByCommodityClass(@PathParam("datasource") String datasource, @PathParam("countryCode") String countryCode, @PathParam("commodityClassCode") String commodityClassCode) throws Exception {

        System.out.println("getCommodity start");
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getCommodityInfoByCommodityClassCode(dsBean, commodityClassCode, countryCode, null);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/condition/{datasource}/{policyDomainCode}/{policyTypeCode}/{commodityDomainCode}")
    public Response getCondition(@PathParam("datasource") String datasource, @PathParam("policyDomainCode") String policyDomainCode, @PathParam("policyTypeCode") String policyTypeCode, @PathParam("commodityDomainCode") String commodityDomainCode) throws Exception {

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getCondition(dsBean, policyDomainCode, policyTypeCode, commodityDomainCode);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/individualPolicy/{datasource}/{policyDomainCode}/{policyTypeCode}/{commodityDomainCode}")
    public Response getIndividualPolicy(@PathParam("datasource") String datasource, @PathParam("policyDomainCode") String policyDomainCode, @PathParam("policyTypeCode") String policyTypeCode, @PathParam("commodityDomainCode") String commodityDomainCode) throws Exception {

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        final JDBCIterablePolicy it =  pp.getIndividualPolicy(dsBean, policyDomainCode, policyTypeCode, commodityDomainCode);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/cplIdForCountry/{datasource}/{countryCode}")
    public Response getCplIdBasedOnCountry(@PathParam("datasource") String datasource, @PathParam("countryCode") String countryCode) throws Exception {

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        //final JDBCIterablePolicy it =  pp.get(dsBean, countryCode);
        final JDBCIterablePolicy it = pp.getDistinctcpl_id_basedOnCountry(dsBean, countryCode);
        final String countryCodeToReturn = countryCode;
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                writer.write(countryCodeToReturn);
                if(it.hasNext()){
                    writer.write(",");
                }
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();

//        // Initiate the stream
//        StreamingOutput stream = new StreamingOutput() {
//
//            @Override
//            public void write(OutputStream os) throws IOException, WebApplicationException {
//
//                // compute result
//                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
//
//                // write the result of the query
//                writer.write("{\"data\": [");
//                while(it.hasNext()) {
//                    // System.out.println(it.next());
//                    String s = g.toJson(it.next());
//                    s = s.substring(1, s.length()-1);
//                    //int comma_index = s.indexOf(",");
//                    //System.out.println(s);
//                    s = "{\"code\":"+s+ ", \"title\":{\"EN\":" +s+"}}";
//
//                    //s = "{\"code\":"+s.substring(0, comma_index)+ ", \"title\":{\"EN\":" +s.substring(comma_index+1)+"}}";
//
//                    writer.write(s);
//                    if (it.hasNext())
//                        writer.write(",");
//                }
//                writer.write("]}");
//
//                // Convert and write the output on the stream
//                writer.flush();
//
//            }
//        };
//        // Stream result
//        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sharedGroupInfo")
    // 'ShareGroupData' = Share Group information
    public Response getSharedGroupInfo(final @FormParam("pdObj") String pdObject) {

        System.out.println("getSharedGroupInfo start ");

        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);

        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        //Share Group
        System.out.println("getSharedGroupInfo before call getSharedGroupInfoByCommodityList ");
        final Map<String, Map<String, LinkedList<String>>> commodity_info =  pp.getSharedGroupInfoByCommodityList(dsBean, pd_obj);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                // write the result of the query
                writer.write("[");
//                while(it.hasNext()) {
//                    //  System.out.println("in loop next i="+i+" elem "+it.next());
//
//                    writer.write(g.toJson(it.next()));
//                    if (it.hasNext())
//                        writer.write(",");
//                    i++;
//                }

                //All the share group
                Set<String> keySet = commodity_info.keySet();
                int len = keySet.size();
                System.out.println("getSharedGroupInfo keySet.size()= "+keySet.size()+" ");
                int i=0;
                for(String key: keySet) {
                    System.out.println("getSharedGroupInfo key "+key);
                    //writer.write("[");

                    //For each share group
                    Set<String> keySet2 = commodity_info.get(key).keySet();
//                    if((key==null)||(key.isEmpty()))
//                    {
//                        writer.write("");
//                    }
//                    else
//                    {
//                        writer.write(key);
//                    }
                    int j=0;
                    int len2 = keySet2.size();
                    for(String key2 : keySet2) {
                        writer.write("[");
                        LinkedList<String> l = commodity_info.get(key).get(key2);
                        if((key2==null)||(key2.isEmpty()))
                        {
                            writer.write("");
                        }
                        else
                        {
                            writer.write("\""+key2+"\"");
                        }
                        writer.write(",");
                        for(int z=0; z<l.size();z++){
                            if((l==null)||(l.isEmpty())||(l.get(z)==null))
                            {
                                writer.write("");
                            }
                            else
                            {
                                String app = l.get(z);
                                System.out.println("app " +app);
                                if(z==4){
//                                    if(app.substring(0,1).contains("\\s+")){
//                                        System.out.println("REPLACE " );
//                                        app = " "+ app;//   ("\\(-\\)", "")
//                                    }
                                    System.out.println("APP before");
                                    System.out.println(app);
                                    app = app.replaceAll(bracketRegex, "");
                                    System.out.println("APP after");
                                    System.out.println(app);
                                    if((app!=null)&&(app.length()>0)){
                                        if(app.substring(0,1)=="("){
                                            app = " "+ app;
                                        }
                                        if(app.substring(0,1)=="\\"){
                                            app = " "+ app;
                                        }
                                    }
                                }

//                                System.out.println("key " +key+" l.get(z)= "+l.get(z));
                                //writer.write(app);
                                writer.write("\""+app+"\"");
                            }

                            if (z<(l.size()-1))
                                writer.write(",");
                        }
                        writer.write("]");

                        if (j<(len2-1))
                            writer.write(",");
                        j++;
                    }
//                    writer.write("]");
//                    if (i<(len-1))
//                        writer.write(",");
                    i++;
                }

                writer.write("]");

                //Convert and write the output on the stream
                System.out.println("END");
                writer.flush();
            }
        };

        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/dataManagementTool/getCpl_id")
    public Response get_cpl_id(@FormParam("pdObj") String pdObject) throws Exception {

        System.out.println(" get_cpl_id START");
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
        System.out.println(" get_cpl_id START 2");
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

        String policy_type = "";
        if (pd_obj.getPolicy_type_code() != null)
        {
            for(int i=0; i< pd_obj.getPolicy_type_code().length; i++)
            {
                policy_type+=pd_obj.getPolicy_type_code()[i];
                if(i<pd_obj.getPolicy_type_code().length-1)
                {
                    policy_type+=",";
                }
            }
        }

        String policy_measure = "";
        if (pd_obj.getPolicy_measure_code() != null)
        {
            for(int i=0; i< pd_obj.getPolicy_measure_code().length; i++)
            {
                policy_measure+=pd_obj.getPolicy_measure_code()[i];
                if(i<pd_obj.getPolicy_measure_code().length-1)
                {
                    policy_measure+=",";
                }
            }
        }

        System.out.println(" get_cpl_id before getDistinctcpl_id_AllInfo");
        final JDBCIterablePolicy it =  pp.getDistinctcpl_id_AllInfo(dsBean, pd_obj, policy_type, policy_measure);

        String cpl_id = "";
        while(it.hasNext()) {
            //[2, Domestic, 1, Agricultural]
            String val = it.next().toString();
            int index = val.lastIndexOf(']');
            val = val.substring(1, index);
            cpl_id +=val;
            cpl_id +=",";
//                val = val.replaceAll("\\s+", "");
        }

        if((cpl_id==null)||(cpl_id.length()==0))
        {
            //  System.out.println("(s.length()==0)");
            //It is not a share group
            // Initiate the stream
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    // compute result
                    Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                    // write the result of the query
                    writer.write("[");
                    writer.write("\"NOT_FOUND\"");
                    writer.write("]");
                    // System.out.println("writer "+writer.toString());
                    // Convert and write the output on the stream
                    writer.flush();
                }

            };
            // Stream result
            return Response.status(200).entity(stream).build();
        }
        else
        {
            final String unique_cpl_id = cpl_id.substring(0, cpl_id.length()-1);
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    // compute result
                    Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                    // write the result of the query
                    writer.write("[");
                    writer.write(""+unique_cpl_id);
                    writer.write("]");
                    // System.out.println("writer "+writer.toString());
                    // Convert and write the output on the stream
                    writer.flush();
                }

            };
            // Stream result
            return Response.status(200).entity(stream).build();
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/dataManagementTool/getPolicyByCplId")
    public Response getPolicyByCplId(@FormParam("pdObj") String pdObject) throws Exception {
        //  System.out.println(" getDownloadPreview start");
        //Gson g = new Gson();
        //  System.out.println(" getDownloadPreview after gson");
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);
//        System.out.println(" getDownloadPreview after class");
//        System.out.println(" pd_obj datasource "+pd_obj.getDatasource());
//        System.out.println(" pd_obj policy_domain_code "+pd_obj.getPolicy_domain_code());
//        System.out.println(" pd_obj commodity_domain_code "+pd_obj.getCommodity_domain_code());
//        System.out.println(" pd_obj commodity_class_code "+pd_obj.getCommodity_class_code());
//        System.out.println(" pd_obj policy_type_code "+pd_obj.getPolicy_type_code());
//        for(int i=0; i<pd_obj.getPolicy_type_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_type_code i "+i+" ="+pd_obj.getPolicy_type_code()[i]);
//        }
//        System.out.println(" pd_obj policy_measure_code "+pd_obj.getPolicy_measure_code());
//        for(int i=0; i<pd_obj.getPolicy_measure_code().length;i++)
//        {
//            System.out.println(" pd_obj policy_measure_code i "+i+" ="+pd_obj.getPolicy_measure_code()[i]);
//        }
//        System.out.println(" pd_obj country_code "+pd_obj.getCountry_code());
//        System.out.println(" pd_obj yearTab "+pd_obj.getYearTab());
//        System.out.println(" pd_obj year_list "+pd_obj.getYear_list());
        System.out.println(" pd_obj start_date "+pd_obj.getStart_date());
        System.out.println(" pd_obj end_date "+pd_obj.getEnd_date());
        System.out.println(" pd_obj date_of_publication"+pd_obj.getDate_of_publication());
        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());
        boolean with_commodity_id = false;
        final JDBCIterablePolicy it =  pp.getPolicyListFromCplId(dsBean, pd_obj);

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    // System.out.println("in loop next");
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");

                // Convert and write the output on the stream
                writer.flush();

            }

        };
        // Stream result
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/createNewSingleCommodity")
    // 'ShareGroupData' = Share Group information
    public Response createNewSingleCommodity(final @FormParam("pdObj") String pdObject) {

        System.out.println("createNewSingleCommodity start ");
//
//        commodity_id        | integer                | not null
//        country_name        | character varying(255) |
//        hs_code             | character varying(255) |
//        hs_suffix           | character varying(255) |
//        hs_version          | character varying(255) |
//        description         | character varying(255) |
//        short_description   | character varying(255) |
//        commodityclass_name | character varying(255) |
//        commodityclass_code | integer                |
//        shared_group_code   | character varying(255) |
//        country_code        | integer                |

        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);

        System.out.println(pd_obj.getCountry_code());
        System.out.println(pd_obj.getCountry_name());
        System.out.println(pd_obj.getHs_code());
//        System.out.println(pd_obj.getHs_suffix());
        System.out.println(pd_obj.getHs_version());
        System.out.println(pd_obj.getDescription());
        System.out.println(pd_obj.getShort_description());
        System.out.println(pd_obj.getCommodity_class_code());
        System.out.println(pd_obj.getCommodity_class_name());
        System.out.println(pd_obj.getShared_group_code());

        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

        try {
            pp.createNewSingleCommodity(dsBean, pd_obj);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                // write the result of the query
                 writer.write(g.toJson("Result: Commodity Created"));
//                writer.write("[NEW COMMODITY CREATED]");

                //Convert and write the output on the stream
                System.out.println("END");
                writer.flush();
            }
        };

        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/createNewSharedGroup")
    // 'ShareGroupData' = Share Group information
    public Response createNewSharedGroup(final @FormParam("pdObj") String pdObject) {

        System.out.println("createNewSharedGroup start ");
//
//        commodity_id        | integer                | not null
//        country_name        | character varying(255) |
//        hs_code             | character varying(255) |
//        hs_suffix           | character varying(255) |
//        hs_version          | character varying(255) |
//        description         | character varying(255) |
//        short_description   | character varying(255) |
//        commodityclass_name | character varying(255) |
//        commodityclass_code | integer                |
//        shared_group_code   | character varying(255) |
//        country_code        | integer                |

        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);

        System.out.println(pd_obj.getCountry_code());
        System.out.println(pd_obj.getCountry_name());
        System.out.println(pd_obj.getDescription());
        System.out.println(pd_obj.getShort_description());
        System.out.println(pd_obj.getCommodity_class_code());
        System.out.println(pd_obj.getCommodity_class_name());
        System.out.println(pd_obj.getCommodity_list());
        System.out.println(pd_obj.getShared_group_code());
//        commodity_list

        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

        try {
            pp.createNewSharedGroup(dsBean, pd_obj);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                // write the result of the query
                writer.write(g.toJson("Result: Commodity Created"));
//                writer.write("[NEW COMMODITY CREATED]");

                //Convert and write the output on the stream
                System.out.println("END");
                writer.flush();
            }
        };

        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
//    @Path("/subnationalMaxCodeNotGaul/{countryCode}")
    @Path("/subnationalMaxCodeNotGaul/{datasource}")
    public Response getSubnationalMaxCodeNotGaul(@PathParam("datasource") String datasource) throws Exception {
//    public Response getSubnationalMaxCodeNotGaul(@PathParam("datasource") String datasource, @PathParam("countryCode") String countryCode) throws Exception {

        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        //final JDBCIterablePolicy it =  pp.get(dsBean, countryCode);
        final JDBCIterablePolicy it = pp.getMaxCodeNotGaul(dsBean);
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
//    @Path("/subnationalMaxCodeNotGaul/{countryCode}")
    @Path("/conditionMaxCode/{datasource}")
    public Response getConditionMaxCode(@PathParam("datasource") String datasource) throws Exception {
//    public Response getSubnationalMaxCodeNotGaul(@PathParam("datasource") String datasource, @PathParam("countryCode") String countryCode) throws Exception {

        System.out.println("getConditionMaxCode start");
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        //final JDBCIterablePolicy it =  pp.get(dsBean, countryCode);
        final JDBCIterablePolicy it = pp.getConditionMaxCode(dsBean);
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    //Without saving commodity_id
                    // String commodity_id = it3.next().toString();
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/dataManagementTool/cplIdMaxCode/{datasource}")
    public Response getCplIdMaxCode(@PathParam("datasource") String datasource) throws Exception {

        System.out.println("getCplIdMax start");
        DatasourceBean dsBean = datasourcePool.getDatasource(datasource);
        //final JDBCIterablePolicy it =  pp.get(dsBean, countryCode);
        final JDBCIterablePolicy it = pp.getCplIdMax(dsBean);
        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));

                // write the result of the query
                writer.write("[");
                while(it.hasNext()) {
                    writer.write(g.toJson(it.next()));
                    if (it.hasNext())
                        writer.write(",");
                }
                writer.write("]");
                // Convert and write the output on the stream
                writer.flush();
                // Stream result
            }

        };
        //  System.out.println("stream "+stream);
        return Response.status(200).entity(stream).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/dataManagementTool/save")
    public Response save(final @FormParam("pdObj") String pdObject) {

        System.out.println("dataManagementTool save start ");
        POLICYDataObject pd_obj = g.fromJson(pdObject, POLICYDataObject.class);

        System.out.println(pd_obj.getLoggedUser());
        System.out.println(pd_obj.getSaveAction());
        System.out.println(pd_obj.getSave_fields());
        LinkedHashMap<String, LinkedHashMap<String, String>> fieldsToSave2 = pd_obj.getSave_fields();

        Set<String> keySet = fieldsToSave2.keySet();
        Iterator<String> itFields = keySet.iterator();

        while(itFields.hasNext()){
            String key = itFields.next();
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Key = "+ key);
            LinkedHashMap<String, String> listValue = fieldsToSave2.get(key);
            Set<String> innerKeySet = listValue.keySet();
            Iterator<String> itInnerFields = innerKeySet.iterator();
            while(itInnerFields.hasNext()) {
                String innerKey = itInnerFields.next();
                String value = listValue.get(innerKey);
                System.out.println("Key = "+ innerKey+ " Value = "+value);
            }
        }

        DatasourceBean dsBean = datasourcePool.getDatasource(pd_obj.getDatasource());

        try {
            //Check if the Cpl_id is already in the master table
            //If it'there saving only the policy
            //if it's not there saving cpl_id in the master table and policy
            LinkedHashMap<String, LinkedHashMap<String, String>> fieldsToSave = pd_obj.getSave_fields();
            LinkedHashMap<String, String> policyN = fieldsToSave.get("policyN");
            String keyToSave = "master";
            if(policyN!=null){
                String cpl_id = policyN.get("CplId");
                pp.saveInMasterTable(dsBean, pd_obj, keyToSave);
                if((cpl_id!=null)&&(cpl_id.length()>0)){
                    final JDBCIterablePolicy itCplId =  pp.getMasterFromCplId(dsBean, pd_obj);
                    if(itCplId.hasNext()){
                        //Cpl is already there
                    }
                    else{
                        //Save in the master
                        pp.saveInMasterTable(dsBean, pd_obj, keyToSave);
                    }
                }
                keyToSave = "policyN";
                pp.saveInPolicyTable(dsBean, pd_obj, keyToSave);
                LinkedHashMap<String, String> policyN_1 = fieldsToSave.get("policyN_1");
                if((policyN_1!=null)&&(policyN_1!=null)){
                    keyToSave = "policyN_1";
                    pp.saveInPolicyTable(dsBean, pd_obj, keyToSave);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Initiate the stream
        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                // compute result
                Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                // write the result of the query
                writer.write(g.toJson("Result: Commodity Created"));
//                writer.write("[NEW COMMODITY CREATED]");

                //Convert and write the output on the stream
                System.out.println("END");
                writer.flush();
            }
        };

        return Response.status(200).entity(stream).build();
    }
}