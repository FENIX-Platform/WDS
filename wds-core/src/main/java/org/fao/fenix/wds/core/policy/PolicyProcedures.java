package org.fao.fenix.wds.core.policy;


import com.google.gson.Gson;
import org.apache.poi.util.SystemOutLogger;
import org.fao.fenix.wds.core.bean.DatasourceBean;
import org.fao.fenix.wds.core.datasource.DatasourcePool;
import org.fao.fenix.wds.core.jdbc.JDBCIterable;
import org.fao.fenix.wds.core.bean.policy.POLICYDataObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author <a href="mailto:barbara.cintoli@fao.org">Barbara Cintoli</a>
 * @author <a href="mailto:barbara.cintoli@gmail.com">Barbara Cintoli</a>
 */
@Component
public class PolicyProcedures {

    @Autowired
    private DatasourcePool datasourcePool;

    private final Gson g = new Gson();

    public JDBCIterablePolicy getcpl_id(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("EXEC Warehouse.dbo.usp_GetODADonors @tablelanguage='");
//        sb.append(lang);
//        sb.append("' ");
        sb.append("SELECT DISTINCT(cpl_id) FROM mastertable");

        // sb.append("SELECT cpl_id, commodity_id  FROM mastertable");
        sb.append("");
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getDistinctcpl_id(DatasourceBean dsBean, POLICYDataObject policyDataObject, String policy_type, String policy_measure) throws Exception {

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("EXEC Warehouse.dbo.usp_GetODADonors @tablelanguage='");
//        sb.append(lang);
//        sb.append("' ");
        sb.append("SELECT DISTINCT(cpl_id) FROM mastertable where country_code =" +policyDataObject.getCountry_code() +" and commodityclass_code ="+policyDataObject.getCommodity_class_code());
        sb.append(" and policytype_code IN ("+policy_type+") and policymeasure_code IN ("+policy_measure+")");
        System.out.println("getDistinctcpl_id sb.toString() = "+sb.toString());

        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getDistinctcpl_id_withDomains(DatasourceBean dsBean, POLICYDataObject policyDataObject, String policy_type, String policy_measure) throws Exception {

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("EXEC Warehouse.dbo.usp_GetODADonors @tablelanguage='");
//        sb.append(lang);
//        sb.append("' ");
        sb.append("SELECT DISTINCT(cpl_id) FROM mastertable where country_code =" +policyDataObject.getCountry_code());
        sb.append(" and commoditydomain_code = "+ policyDataObject.getCommodity_domain_code()+" and commodityclass_code ="+policyDataObject.getCommodity_class_code());
        sb.append(" and policydomain_code = "+ policyDataObject.getPolicy_domain_code()+" and policytype_code IN ("+policy_type+") and policymeasure_code IN ("+policy_measure+")");
        System.out.println("getDistinctcpl_id_withDomains sb.toString() = "+sb.toString());

        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getDistinctcpl_id_AllInfo(DatasourceBean dsBean, POLICYDataObject policyDataObject, String policy_type_code, String policy_measure_code) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT DISTINCT(mastertable.cpl_id) FROM mastertable, mastertableB");
//        sb.append(" where commodity_id IN ("+ policyDataObject.getCommodity_id()+") AND country_code IN ("+ policyDataObject.getCountry_code()+") AND mastertableB.subnational_code IN ("+ policyDataObject.getSubnational_code()+")");
        sb.append(" where commodity_id IN (108) AND country_code IN ("+ policyDataObject.getCountry_code()+") AND mastertableB.subnational_code IN ("+ policyDataObject.getSubnational_code()+")");
        sb.append(" AND commoditydomain_code IN ("+ policyDataObject.getCommodity_domain_code()+") AND commodityclass_code IN ("+ policyDataObject.getCommodity_class_code()+") AND policydomain_code IN ("+ policyDataObject.getPolicy_domain_code()+")");
        sb.append(" AND policytype_code IN ("+ policy_type_code+") AND policymeasure_code IN ("+ policy_measure_code+") AND condition_code IN ("+ policyDataObject.getCondition_code()+") AND individualpolicy_code IN ("+ policyDataObject.getIndividualpolicy_code()+")");
        sb.append(" AND mastertable.cpl_id = mastertableB.cpl_id");
        sb.append(" ORDER BY mastertable.cpl_id ASC");

//        sb.append("SELECT DISTINCT(mastertable.cpl_id) FROM mastertable, mastertableB");
////        sb.append(" where commodity_id IN ("+ policyDataObject.getCommodity_id()+") AND country_code IN ("+ policyDataObject.getCountry_code()+") AND mastertableB.subnational_code IN ("+ policyDataObject.getSubnational_code()+")");
//        sb.append(" where commodity_id IN (108) AND country_code IN ("+ policyDataObject.getCountry_code()+") AND mastertableB.subnational_code IN ("+ policyDataObject.getSubnational_code()+")");
//        sb.append(" AND commoditydomain_code IN ("+ policyDataObject.getCommodity_domain_code()+") AND commodityclass_code IN ("+ policyDataObject.getCommodity_class_code()+") AND policydomain_code IN ("+ policyDataObject.getPolicy_domain_code()+")");
//        sb.append(" AND policytype_code IN ("+ policy_type_code+") AND policymeasure_code IN ("+ policy_measure_code+") AND condition_code IN ("+ policyDataObject.getCondition_code()+") AND individualpolicy_code IN ("+ policyDataObject.getIndividualpolicy_code()+"))");
//        sb.append(" from mastertable JOIN mastertableB ON mastertable.cpl_id = mastertableB.cpl_id where order by mastertable.cpl_id ASC");
//        sb.append(" ORDER BY mastertable.cpl_id ASC");
        System.out.println("getDistinctcpl_id_AllInfo sb.toString() = "+sb.toString());

        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getDistinctcpl_id_basedOnCountry(DatasourceBean dsBean, String country_code) throws Exception {

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT DISTINCT(cpl_id) FROM mastertable where country_code IN (" +country_code+")");
        sb.append("ORDER BY cpl_id ASC");
        System.out.println("getDistinctcpl_id_basedOnCountry sb.toString() = "+sb.toString());

        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getcountries_fromPolicy(DatasourceBean dsBean, String policyType, String policyMeasure)throws Exception {

        //SELECT country_code, country_name FROM mastertable where commodityclass_code =3 and policytype_code IN (1) and policymeasure_code IN (3);
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT DISTINCT(country_code), country_name FROM mastertable where policytype_code IN ("+policyType+") and policymeasure_code IN ("+policyMeasure+") ORDER BY country_name ASC");

        System.out.println(sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getpolicyAndcommodityDomain(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("EXEC Warehouse.dbo.usp_GetODADonors @tablelanguage='");
//        sb.append(lang);
//        sb.append("' ");
//        sb.append("SELECT DISTINCT(cpl_id) FROM mastertable");

//        sb.append("SELECT DISTINCT(policydomain_name), commoditydomain_name from mastertable");
        sb.append("SELECT DISTINCT(policydomain_code), policydomain_name, commoditydomain_code, commoditydomain_name from mastertable");
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getstartAndEndDate(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("EXEC Warehouse.dbo.usp_GetODADonors @tablelanguage='");
//        sb.append(lang);
//        sb.append("' ");
//        sb.append("SELECT DISTINCT(cpl_id) FROM mastertable");

//        sb.append("SELECT MIN(start_date) from policytable");
//        sb.append("SELECT MAX(end_date) from policytable");
//        sb.append("SELECT MIN(start_date), MAX(end_date) from policytable");
        //sb.append("SELECT MAX(end_date), MIN(EXTRACT(day FROM start_date)), MIN(EXTRACT(month FROM start_date)), MIN(EXTRACT(year FROM start_date)), MAX(EXTRACT(day FROM start_date)), MAX(EXTRACT(month FROM start_date)), MAX(EXTRACT(year FROM start_date)) from policytable");
        sb.append("SELECT MAX(GREATEST(start_date,end_date)), MIN(EXTRACT(day FROM start_date)), MIN(EXTRACT(month FROM start_date)), MIN(EXTRACT(year FROM start_date)), EXTRACT(day FROM MAX(GREATEST(start_date,end_date))), EXTRACT(month FROM MAX(GREATEST(start_date,end_date))), EXTRACT(year FROM MAX(GREATEST(start_date,end_date))) from policytable");
        // System.out.println("getstartAndEndDate sb="+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getEndDateIsNull(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT DISTINCT(end_date) FROM policytable WHERE end_date IS NULL");
        it.query(dsBean, sb.toString());
        //System.out.println("getEndDateIsNull After query "+sb.toString());
        return it;
    }

    public JDBCIterablePolicy getpolicyTypes(DatasourceBean dsBean, String policyDomainCodes, String commodityDomainCodes) throws Exception {
//        System.out.println("getpolicyTypes policyDomainCodes "+policyDomainCodes);
//        System.out.println("getpolicyTypes commodityDomainCodes "+commodityDomainCodes);
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("EXEC Warehouse.dbo.usp_GetODADonors @tablelanguage='");
//        sb.append(lang);
//        sb.append("' ");
        sb.append("SELECT DISTINCT(policytype_code), policytype_name FROM mastertable where policydomain_code IN ("+policyDomainCodes+") AND commoditydomain_code IN ("+commodityDomainCodes+") order by policytype_name ASC ");
        //  System.out.println("getpolicyTypes sb "+sb);
        // sb.append("SELECT cpl_id, commodity_id  FROM mastertable");
        sb.append("");
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getDownloadPreview(DatasourceBean dsBean, POLICYDataObject pd_obj, boolean with_commodity_id) throws Exception{

//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        StringBuilder sbApp = new StringBuilder();

        String policyMeasuresCodesArray[] = pd_obj.getPolicy_measure_code();
        boolean unique = true;
        if((policyMeasuresCodesArray!=null)&&(policyMeasuresCodesArray.length>0))
        {
            int unionCount = 0;
            for(int i=0; i<policyMeasuresCodesArray.length;i++)
            {
                // System.out.println("policyMeasuresCodesArray[i] "+policyMeasuresCodesArray[i]);
                String policyMeasuresCodes = policyMeasuresCodesArray[i];
                if(!policyMeasuresCodes.isEmpty())
                {
                    unique = false;
                    if(sbApp.length()>0)
                    {
                        sbApp.append(" UNION ");
                    }
                    //   System.out.println("pd_obj.getPolicy_type_code()[i] "+pd_obj.getPolicy_type_code()[i]);
                    sbApp.append("("+this.getCplId_downloadPreview(pd_obj, pd_obj.getPolicy_type_code()[i], policyMeasuresCodes, with_commodity_id)+")");
                    unionCount++;
                }
            }
            if(unique)
            {
                //There are not Policy Measures Codes... so unique query
                sb.append(this.getCplId_downloadPreview(pd_obj, "", "", with_commodity_id));
            }
            else{
                if(unionCount>1)
                {
                    sb.append("SELECT * FROM (");
                    sb.append(sbApp);
                    sb.append(") t ORDER BY t.cpl_id ASC");
                }
                else
                {
                    //Case with one policy type
                    //Removing the first and the end brackets
                    String sb_withoutBrackets = sbApp.substring(1);
                    int last_bracket_index = sb_withoutBrackets.lastIndexOf(")");
                    sb_withoutBrackets = sb_withoutBrackets.substring(0,last_bracket_index);
                    sb.append(sb_withoutBrackets);
                }
            }
        }

        it.query(dsBean, sb.toString());
        return it;
    }

    public StringBuilder getCplId_downloadPreview(POLICYDataObject pd_obj, String policy_type_code, String policy_measure_code, boolean with_commodity_id){

        StringBuilder sb = new StringBuilder();
        if(with_commodity_id)
        {
            sb.append("SELECT DISTINCT(mastertable.cpl_id), mastertable.commodity_id FROM mastertable, policytable ");
        }
        else
        {
            sb.append("SELECT DISTINCT(mastertable.cpl_id) FROM mastertable, policytable ");
        }
        sb.append("WHERE mastertable.cpl_id = policytable.cpl_id AND ");
        //Mandatory Fields
        sb.append("commoditydomain_code IN ("+pd_obj.getCommodity_domain_code()+") AND ");
        sb.append("policydomain_code IN ("+pd_obj.getPolicy_domain_code()+") ");
        // System.out.println("year tab = "+pd_obj.getYearTab());
        if(pd_obj.getYearTab().equals("classic"))
        {
            if((pd_obj.getYear_list()!=null)&&(!pd_obj.getYear_list().isEmpty()))
            {
                sb.append("AND ((EXTRACT(year FROM start_date) IN ("+pd_obj.getYear_list()+")) OR (EXTRACT(year FROM end_date) IN ("+pd_obj.getYear_list()+")))");
            }
        }
        else
        {   //Format: 01/03/2014
            String start_date = pd_obj.getStart_date();
            String end_date = pd_obj.getEnd_date();
            //((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010'))))
            sb.append("AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"')  OR ((policytable.start_date <= '"+start_date+"') AND (policytable.end_date>='"+end_date+"')) OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//            sb.append("((policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"') OR (policytable.end_date >= '"+end_date+"' AND policytable.end_date <= '"+end_date+"') OR (policytable.end_date IS NULL AND (policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"')))");
        }
        //Optional Fields
        if((pd_obj.getCommodity_class_code()!=null)&&(pd_obj.getCommodity_class_code().length()>0))
        {
            sb.append(" AND commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
        }
        if((policy_type_code!=null)&&(policy_type_code.length()>0))
        {
            sb.append(" AND policytype_code IN ("+policy_type_code+")");
        }
        if((policy_measure_code!=null)&&(policy_measure_code.length()>0))
        {
            sb.append(" AND policymeasure_code IN ("+policy_measure_code+")");
        }
        if((pd_obj.getCountry_code()!=null)&&(pd_obj.getCountry_code().length()>0))
        {
            sb.append(" AND country_code IN ("+pd_obj.getCountry_code()+")");
        }
        sb.append(" ORDER BY mastertable.cpl_id ASC");

        System.out.println("getCplId_downloadPreview end  "+sb);
        return sb;
    }

    //This function returns all the master fields using the cpl_id
    public JDBCIterablePolicy getMasterFromCplId(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{

//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT * from mastertable where cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns all the master fields using the cpl_id
    public LinkedList<String[]> getMasterFromCplIdAndSubnational(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        JDBCIterablePolicy it2 = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        //System.out.println("getMasterFromCplIdAndSubnational pd_obj.getCpl_id() *"+pd_obj.getCpl_id()+"*");
//        sb.append("SELECT * from mastertable where cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");
//        sb.append("SELECT mastertable.cpl_id, cpl_code, commodity_id, country_code, country_name, mastertableB.subnational_code, mastertableB.subnational_code, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name from mastertable JOIN mastertableB ON mastertable.cpl_id = mastertableB.cpl_id where mastertable.cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");
        sb.append("SELECT mastertable.cpl_id, commodity_id, country_code, country_name, mastertableB.subnational_code, mastertableB.subnational_code, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name from mastertable JOIN mastertableB ON mastertable.cpl_id = mastertableB.cpl_id where mastertable.cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");
        it.query(dsBean, sb.toString());

        final JDBCIterablePolicy itApp = it;
        LinkedHashMap<String, String> subnational_map = pd_obj.getSubnationalMap();
        LinkedHashMap<String, String> subnational_map_level_3 = pd_obj.getSubnationalMap_level_3();
//        if((subnational_map!=null)||(subnational_map.size()>0))
//        {
//            System.out.println("Map not null");
//            System.out.println("subnational_map size = "+subnational_map.size());
//        }
//        else{
//            System.out.println("Map null");
//        }
        Set<String> keyset = subnational_map.keySet();
        //System.out.println("keyset size = "+keyset.size());
        Iterator<String> itS = keyset.iterator();
        while(itS.hasNext()){
            String key = itS.next();
            //System.out.println("Key = "+key +" value = "+ subnational_map.get(key));
        }

        //System.out.println("Before loop ");
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        LinkedList<String[]> totalList = new LinkedList<String[]>();
        int k=0;
        while(itApp.hasNext()) {
            List<String> valueList = itApp.next();
            System.out.println("valueList "+valueList.toString());
            //There is only one value
           // String value = valueList.toString();
            //String s = (value.substring(1,(value.length()-1))).replaceAll("\\s","");
            //String s = value.substring(1,(value.length()-1));
//            String[] ar = s.split(",");
//            for(int i=0; i< ar.length;i++){
//                if(ar[i].substring(0,1).equals(" "))
//                {
//                    ar[i] = ar[i].substring(1);
//                    //System.out.println("*"+ar[i]+"*");
//                }
//            }

            String[] ar = new String[valueList.size()];
            for(int i=0; i< valueList.size();i++){
                String value = valueList.get(i);
                if(value!=null){
                    String valueToString = value.toString();
                    if(valueToString.substring(0,1).equals(" "))
                    {
                        ar[i] = valueToString.substring(1);
                        //System.out.println("*"+ar[i]+"*");
                    }
                    else {
                        ar[i] = valueToString;
                    }
                }
                else{
                    ar[i] = "";
                }
            }
            k++;
            //System.out.println("ar "+ar.toString());
            String[] newAr = new String[(ar.length+1)];
            //*17, AR99_AgTr_ExXban_131_105_999, 131, 12, Argentina, 99, 1, Agricultural, 3, Maize, 1, Trade, 1, Export measures, 1, Export prohibition, 105, none, 999, none*
            int newArrayInt = 0;
            //System.out.println("ar len="+ar.length);
            for(int i=0; i< ar.length; i++){

                if((i==4)||(i==5)){
                    //Subnational code
                    int subnational_code_index = 4;
                    int subnational_name_index = 5;

                    if((ar[subnational_code_index]!=null)&&(ar[subnational_code_index].equalsIgnoreCase("99"))){
                        //System.out.println("IN LOOP ar[subnational_code_index] "+ar[subnational_code_index]);
                        newAr[subnational_code_index] = ar[subnational_code_index];
                        newAr[subnational_name_index] = "n.a";
                    }
                    else if(Integer.parseInt(ar[subnational_code_index])>=0){
                        //System.out.println("subnational_code_index= "+subnational_code_index);
                        //It's a Gaul code... name from d3s
                        //key = subnational_code, value = subnational_name
//                        System.out.println("IN!!");
//                        System.out.println("ar[subnational_code_index] ="+ar[subnational_code_index]);
//                        System.out.println("subnational_map.get(ar[subnational_code_index]) ="+subnational_map.get(ar[subnational_code_index]));
                        if(subnational_map.get(ar[subnational_code_index])!=null){
                           // System.out.println("if");
                            String subnational_name = subnational_map.get(ar[subnational_code_index]);
                            //System.out.println("subnational_map.get(ar[subnational_code_index]= "+subnational_map.get(ar[subnational_code_index]));
                            newAr[subnational_code_index] = ar[subnational_code_index]+"_Level2";
                            newAr[subnational_name_index] = subnational_name;
                        }
                        else{
                            //System.out.println("else");
                            String subnational_name = subnational_map_level_3.get(ar[subnational_code_index]);
                            newAr[subnational_code_index] = ar[subnational_code_index]+"_Level3";
                            newAr[subnational_name_index] = subnational_name;
                        }
                        //System.out.println("newAr[subnational_name_index] ="+newAr[subnational_name_index]);
                    }
                    else{
                        //Negative Code .... from user... name from policy db
                        StringBuilder sb2 = new StringBuilder();
                        sb2.append("SELECT subnational_name from customsubnationaltable where subnational_code IN ("+ar[subnational_code_index]+")");
//                        System.out.println("In negative");
//                        System.out.println(sb2.toString());
                        it2.query(dsBean, sb2.toString());
                        boolean found = false;
                        while(it2.hasNext()){
                            newAr[subnational_name_index] = it2.next().toString();
                            if(newAr[subnational_name_index].indexOf("[")==0){
                                //Removing []
                                newAr[subnational_name_index]= newAr[subnational_name_index].substring(1,newAr[subnational_name_index].length()-1);
                                newAr[subnational_code_index] = ar[subnational_code_index]+"_LevelNeg";
                            }

                            found = true;
                        }
                        if(!found){
                            newAr[subnational_code_index] = "NotFound";
                            newAr[subnational_name_index] = "NotFound";
                        }
                    }

                    //ar[0] = cpl_id
                    if(map.get(ar[0])!=null){
                        String subnationalCode = map.get(ar[0]);
                        if((subnationalCode!=null)&&(!subnationalCode.contains(newAr[subnational_name_index])))
                            subnationalCode += ","+newAr[subnational_name_index];
                        map.put(ar[0], subnationalCode);
                        //System.out.println("366 if subnationalCode= "+subnationalCode);
                    }
                    else{
                        map.put(ar[0], newAr[subnational_name_index]);
                        //System.out.println("366 else subnationalCode= "+newAr[subnational_name_index]);
                    }
                }
                else{
                    //For the other fields of the array
                    newAr[newArrayInt] = ar[i];
                }

                //System.out.println("ar[i] = "+ar[i]+" newAr[newArrayInt] = "+newAr[newArrayInt]);
                newArrayInt++;
            }
//            System.out.println(newAr.toString());
            totalList.add(newAr);
        }
        LinkedList<String[]> totalListToReturn = new LinkedList<String[]>();

        System.out.println("MAP!!!");
        System.out.println(map);
        //Loop on the list to create a unique string
        for(int i = 0; i< totalList.size(); i++){

            //System.out.println("totalList.get(i)[0]  = "+totalList.get(i)[0]);
            String cpl_id = totalList.get(i)[0];
            //System.out.println("totalList.get(i)[6] before = "+totalList.get(i)[6]);
            if((map.get(cpl_id)!=null)&&(map.get(cpl_id).contains("n.a"))){
                totalList.get(i)[5] = "n.a";
            }
            else{
                totalList.get(i)[5] = map.get(cpl_id);
            }

            //System.out.println("totalList.get(i)[6] after = "+totalList.get(i)[6]);
            //System.out.println("totalList i  "+totalList.get(i).toString());
            boolean found_in_list = false;
            for(int iList=0; iList<totalListToReturn.size(); iList++){

                String elem[] = totalListToReturn.get(iList);
//                System.out.println(elem[6]);
//                System.out.println(totalList.get(i)[0]);
                if((elem[0]!=null)&&(elem[0].equals(totalList.get(i)[0]))){
                    //That cpl_id is already in the list
                    found_in_list = true;
                    break;
                }
            }

            if(found_in_list==false)
            {
//                System.out.println("Before add.... "+totalList.get(i)[5]+"...."+totalList.get(i)[6]);
                totalListToReturn.add(totalList.get(i));
            }
        }
       // System.out.println("*"+s+"*");
//                    value = value.substring(2, value.lastIndexOf("\""));
//                   s+= "'"+g.toJson(it2.next())+"'";
//                    s+= "'"+it2.next()+"'";

     //   s+= value.substring(1,(value.length()-1));
        System.out.println("getMasterFromCplIdAndSubnational end");
        System.out.println(totalListToReturn);
        return totalListToReturn;
    }

    //This function returns all the master fields using the cpl_id
    public LinkedList<String[]> getMasterFromCplIdAndNegativeSubnational(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        JDBCIterablePolicy it2 = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        //System.out.println("getMasterFromCplIdAndSubnational pd_obj.getCpl_id() *"+pd_obj.getCpl_id()+"*");
//        sb.append("SELECT * from mastertable where cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");
//        sb.append("SELECT mastertable.cpl_id, cpl_code, commodity_id, country_code, country_name, mastertableB.subnational_code, mastertableB.subnational_code, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name from mastertable JOIN mastertableB ON mastertable.cpl_id = mastertableB.cpl_id where mastertable.cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");
        sb.append("SELECT mastertable.cpl_id, commodity_id, country_code, country_name, mastertableB.subnational_code, mastertableB.subnational_code, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name from mastertable JOIN mastertableB ON mastertable.cpl_id = mastertableB.cpl_id where mastertable.cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");

        //System.out.println("getMasterFromCplIdAndSubnational sb "+sb.toString());
        it.query(dsBean, sb.toString());

        final JDBCIterablePolicy itApp = it;

        //System.out.println("Before loop ");
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        LinkedList<String[]> totalList = new LinkedList<String[]>();
        int k=0;
        while(itApp.hasNext()) {
            List<String> valueList = itApp.next();
            //System.out.println("valueList "+valueList.toString());
            //There is only one value
            String value = valueList.toString();
            //String s = (value.substring(1,(value.length()-1))).replaceAll("\\s","");
            String s = value.substring(1,(value.length()-1));
            String[] ar = s.split(",");
            for(int i=0; i< ar.length;i++){
                if(ar[i].substring(0,1).equals(" "))
                {
                    ar[i] = ar[i].substring(1);
                    //System.out.println("*"+ar[i]+"*");
                }
            }
            k++;
            //System.out.println("ar "+ar.toString());
            String[] newAr = new String[(ar.length+1)];
            //*17, AR99_AgTr_ExXban_131_105_999, 131, 12, Argentina, 99, 1, Agricultural, 3, Maize, 1, Trade, 1, Export measures, 1, Export prohibition, 105, none, 999, none*
            int newArrayInt = 0;
            for(int i=0; i< ar.length; i++){

                if((i==4)||(i==5)){
                    //Subnational code
                    int subnational_code_index = 4;
                    int subnational_name_index = 5;

                    if(Integer.parseInt(ar[subnational_code_index])<0){
                        //Negative Code .... from user... name from policy db
                        StringBuilder sb2 = new StringBuilder();
                        sb2.append("SELECT subnational_name from customsubnationaltable where subnational_code IN ("+ar[subnational_code_index]+")");
//                        System.out.println("In negative");
//                        System.out.println(sb2.toString());
                        it2.query(dsBean, sb2.toString());
                        boolean found = false;
                        while(it2.hasNext()){
                            newAr[subnational_name_index] = it2.next().toString();
                            if(newAr[subnational_name_index].indexOf("[")==0){
                                //Removing []
                                newAr[subnational_name_index]= newAr[subnational_name_index].substring(1,newAr[subnational_name_index].length()-1);
                                newAr[subnational_code_index] = ar[subnational_code_index]+"_LevelNeg";
                            }

                            found = true;
                        }
                        if(!found){
                            newAr[subnational_code_index] = "NotFound";
                            newAr[subnational_name_index] = "NotFound";
                        }
                    }
                    else{
                        newAr[subnational_code_index] = "NotFound";
                        newAr[subnational_name_index] = "NotFound";
                    }

                    //ar[0] = cpl_id
                    if(map.get(ar[0])!=null){
                        String subnationalCode = map.get(ar[0]);
                        if((subnationalCode!=null)&&(!subnationalCode.contains(newAr[subnational_name_index])))
                            subnationalCode += ","+newAr[subnational_name_index];
                        map.put(ar[0], subnationalCode);
                        //System.out.println("366 if subnationalCode= "+subnationalCode);
                    }
                    else{
                        map.put(ar[0], newAr[subnational_name_index]);
                        //System.out.println("366 else subnationalCode= "+newAr[subnational_name_index]);
                    }
                }
                else{
                    //For the other fields of the array
                    newAr[newArrayInt] = ar[i];
                }

                //System.out.println("ar[i] = "+ar[i]+" newAr[newArrayInt] = "+newAr[newArrayInt]);
                newArrayInt++;
            }
          //  System.out.println("newAr START ");
//            for(int i=0; i< newAr.length; i++){
//                System.out.println("newAr = "+newAr[i]);
//            }
       //     System.out.println("newAr END ");
//            System.out.println(newAr.toString());
            totalList.add(newAr);
        }
        LinkedList<String[]> totalListToReturn = new LinkedList<String[]>();

        System.out.println("MAP!!!");
        System.out.println(map);
        //Loop on the list to create a unique string
        for(int i = 0; i< totalList.size(); i++){

            //System.out.println("totalList.get(i)[0]  = "+totalList.get(i)[0]);
            String cpl_id = totalList.get(i)[0];
            //System.out.println("totalList.get(i)[6] before = "+totalList.get(i)[6]);
            if((map.get(cpl_id)!=null)&&(map.get(cpl_id).contains("n.a"))){
                totalList.get(i)[5] = "n.a";
            }
            else{
                totalList.get(i)[5] = map.get(cpl_id);
            }

            //System.out.println("totalList.get(i)[6] after = "+totalList.get(i)[6]);
            //System.out.println("totalList i  "+totalList.get(i).toString());
            //System.out.println("BEFORE FOR");
            boolean found_in_list = false;
            for(int iList=0; iList<totalListToReturn.size(); iList++){

                String elem[] = totalListToReturn.get(iList);
//                System.out.println(elem[6]);
//                System.out.println(totalList.get(i)[0]);
                if((elem[0]!=null)&&(elem[0].equals(totalList.get(i)[0]))){
                    //That cpl_id is already in the list
                    found_in_list = true;
                    break;
                }
            }
            //System.out.println("AFTER FOR");

            if(found_in_list==false)
            {
                //System.out.println("Before add.... "+totalList.get(i)[5]+"...."+totalList.get(i)[6]);
                totalListToReturn.add(totalList.get(i));
            }
        }
        // System.out.println("*"+s+"*");
//                    value = value.substring(2, value.lastIndexOf("\""));
//                   s+= "'"+g.toJson(it2.next())+"'";
//                    s+= "'"+it2.next()+"'";

        //   s+= value.substring(1,(value.length()-1));
        //System.out.println("getMasterFromCplIdAndSubnational end");
        //System.out.println(totalListToReturn);
        return totalListToReturn;
    }

    public LinkedHashMap<String, String> getCountryMapData(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception {

        LinkedHashMap<String, String> countryMap = new LinkedHashMap<String, String>();

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        //sb.append("SELECT mastertable.cpl_id, cpl_code, commodity_id, country_code, country_name, mastertableB.subnational_code, mastertableB.subnational_code, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name from mastertable JOIN mastertableB ON mastertable.cpl_id = mastertableB.cpl_id where mastertable.cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");

        //sb.append("SELECT "+getSelectForPolicyTable()+" from policytable, commlistwithid where cpl_id IN ("+pd_obj.getCpl_id()+") AND commlistwithid.commodity_id IN ("+pd_obj.getCommodity_id()+") AND policytable.commodity_id=commlistwithid.commodity_id ");

        sb.append("select country_code, country_name, COUNT(policy_id) from policytable, mastertableB, mastertable where mastertableB.cpl_id IN ("+pd_obj.getCpl_id()+") and policytable.cpl_id = mastertableB.cpl_id and policytable.cpl_id = mastertable.cpl_id and mastertableB.subnational_code='99'");

        if(pd_obj.getYearTab().equals("classic"))
        {
            if((pd_obj.getYear_list()!=null)&&(!pd_obj.getYear_list().isEmpty()))
            {
                sb.append("AND ((EXTRACT(year FROM start_date) IN ("+pd_obj.getYear_list()+")) OR (EXTRACT(year FROM end_date) IN ("+pd_obj.getYear_list()+"))) ");
            }
        }
        else
        {   //Format: 01/03/2014
            String start_date = pd_obj.getStart_date();
            String end_date = pd_obj.getEnd_date();
            //((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010'))))
            sb.append("AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"')  OR ((policytable.start_date <= '"+start_date+"') AND (policytable.end_date>='"+end_date+"')) OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//            sb.append("((policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"') OR (policytable.end_date >= '"+end_date+"' AND policytable.end_date <= '"+end_date+"') OR (policytable.end_date IS NULL AND (policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"')))");
        }

        sb.append(" group by country_code, country_name");

        System.out.println("getCountryMapData sb "+sb.toString());
        it.query(dsBean, sb.toString());

        while(it.hasNext()) {
            List<String> valueList = it.next();
            String country = valueList.get(0);
            String policy_count = valueList.get(2);
            countryMap.put(country, policy_count);
        }

        return countryMap;
    }

    public LinkedHashMap<String, String> getSubnationalMapData(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception {

        LinkedHashMap<String, String> subnationalMap = new LinkedHashMap<String, String>();

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        //sb.append("SELECT mastertable.cpl_id, cpl_code, commodity_id, country_code, country_name, mastertableB.subnational_code, mastertableB.subnational_code, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name from mastertable JOIN mastertableB ON mastertable.cpl_id = mastertableB.cpl_id where mastertable.cpl_id IN ("+pd_obj.getCpl_id()+") order by mastertable.cpl_id ASC");

        //sb.append("SELECT "+getSelectForPolicyTable()+" from policytable, commlistwithid where cpl_id IN ("+pd_obj.getCpl_id()+") AND commlistwithid.commodity_id IN ("+pd_obj.getCommodity_id()+") AND policytable.commodity_id=commlistwithid.commodity_id ");

        //sb.append("select country_code, country_name, COUNT(policy_id) from policytable, mastertableB, mastertable where mastertableB.cpl_id IN ("+pd_obj.getCpl_id()+") and policytable.cpl_id = mastertableB.cpl_id and policytable.cpl_id = mastertable.cpl_id and mastertableB.subnational_code='99'");

        sb.append("select subnational_code, COUNT(policy_id) from policytable, mastertableB where policytable.cpl_id IN ("+pd_obj.getCpl_id()+") and policytable.cpl_id = mastertableB.cpl_id and subnational_code<>'99' and CAST(subnational_code AS INTEGER)>0 ");

        if(pd_obj.getYearTab().equals("classic"))
        {
            if((pd_obj.getYear_list()!=null)&&(!pd_obj.getYear_list().isEmpty()))
            {
                sb.append("AND ((EXTRACT(year FROM start_date) IN ("+pd_obj.getYear_list()+")) OR (EXTRACT(year FROM end_date) IN ("+pd_obj.getYear_list()+"))) ");
            }
        }
        else
        {   //Format: 01/03/2014
            String start_date = pd_obj.getStart_date();
            String end_date = pd_obj.getEnd_date();
            //((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010'))))
            sb.append("AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"')  OR ((policytable.start_date <= '"+start_date+"') AND (policytable.end_date>='"+end_date+"')) OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//            sb.append("((policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"') OR (policytable.end_date >= '"+end_date+"' AND policytable.end_date <= '"+end_date+"') OR (policytable.end_date IS NULL AND (policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"')))");
        }

        sb.append(" group by subnational_code");

        System.out.println("getSubnationalMapData sb "+sb.toString());
        it.query(dsBean, sb.toString());

        while(it.hasNext()) {
            List<String> valueList = it.next();
            String country = valueList.get(0);
            String policy_count = valueList.get(1);
            subnationalMap.put(country, policy_count);
        }

        return subnationalMap;
    }

    //This function returns the code and the label of each commodity belonging to the commodity
    // (if the commodity is a share group)
    public JDBCIterablePolicy getshareGroupInfo(DatasourceBean dsBean, String commodity_id) throws Exception{
        //  System.out.println("getshareGroupInfo ");

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        //Through the commodity id get the shared_group_code
        sb.append("SELECT shared_group_code FROM commlistwithid where commodity_id = '"+commodity_id+"'");
         System.out.println("getshareGroupInfo sb "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns the code and the label of each commodity belonging to the commodity
    // (if the commodity is a share group)
    public JDBCIterablePolicy getSingleIdFromCommodityId(DatasourceBean dsBean, String commodity_id) throws Exception{
        //   System.out.println("getSingleIdFromCommodityId ");

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        //Through the commodity id get the shared_group_code
        sb.append("SELECT id_single FROM sharedgroups where commodity_id = '"+commodity_id+"'");
        //  System.out.println("getSingleIdFromCommodityId sb "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

//    //This function returns commodity_id, target_code, description associated to the commodity id
//    public JDBCIterablePolicy getCommodityInfo_commodity_id(DatasourceBean dsBean, String commodity_id_list) throws Exception{
//      //  System.out.println("getCommodityInfo ");
//
//        JDBCIterablePolicy it = new JDBCIterablePolicy();
//        StringBuilder sb = new StringBuilder();
//
//        //Through the commodity id get the shared_group_code
//        //sb.append("SELECT commodity_id, target_code, description FROM commlistwithid where commodity_id IN ("+commodity_id_list+")");
//
//        sb.append("SELECT commodity_id, hs_code, hs_suffix, hs_version, short_description FROM commlistwithid where commodity_id IN ("+commodity_id_list+")");
//        System.out.println("getCommodityInfo_commodity_id "+sb.toString());
//        it.query(dsBean, sb.toString());
//        return it;
//    }

    //commodity_id is the commodity_id of the Share Group
    public JDBCIterablePolicy getCommodityInfo_commodity_id(DatasourceBean dsBean, String commodity_id) throws Exception{
        //  System.out.println("getCommodityInfo ");
        //SELECT v.share_group_commodity_id, commlistwithid.shared_group_code, v.commodity_id, v.hs_code, v.hs_suffix, v.hs_version, v.short_description, v.original_hs_version, v.original_hs_code, v.original_hs_suffix from (SELECT sharedgroups.commodity_id as share_group_commodity_id, commlistwithid.commodity_id, commlistwithid.hs_code, commlistwithid.hs_suffix, commlistwithid.hs_version, commlistwithid.short_description, sharedgroups.original_hs_version, sharedgroups.original_hs_code, sharedgroups.original_hs_suffix FROM commlistwithid INNER JOIN sharedgroups ON commlistwithid.commodity_id=sharedgroups.id_single and sharedgroups.commodity_id = 1037) v where v.share_group_commodity_id = 1037;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

//        sb.append("SELECT commodity_id, hs_code, hs_suffix, hs_version, short_description, shared_group_code FROM commlistwithid where commodity_id IN ("+commodity_id_list+")");
        sb.append(" SELECT * from (SELECT sharedgroups.commodity_id as shared_group_commodity_id, commlistwithid.commodity_id, commlistwithid.hs_code, commlistwithid.hs_suffix, commlistwithid.hs_version, commlistwithid.short_description, sharedgroups.original_hs_version, sharedgroups.original_hs_code, sharedgroups.original_hs_suffix FROM commlistwithid INNER JOIN sharedgroups ON commlistwithid.commodity_id=sharedgroups.id_single and sharedgroups.commodity_id IN ( "+commodity_id+")) v where v.shared_group_commodity_id IN ( "+commodity_id+")");
        System.out.println("getCommodityInfo_commodity_id "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getCommodityInfo_commodity_idWithLongDescription(DatasourceBean dsBean, String commodity_id) throws Exception{
        //  System.out.println("getCommodityInfo ");
        //SELECT v.share_group_commodity_id, commlistwithid.shared_group_code, v.commodity_id, v.hs_code, v.hs_suffix, v.hs_version, v.short_description, v.original_hs_version, v.original_hs_code, v.original_hs_suffix from (SELECT sharedgroups.commodity_id as share_group_commodity_id, commlistwithid.commodity_id, commlistwithid.hs_code, commlistwithid.hs_suffix, commlistwithid.hs_version, commlistwithid.short_description, sharedgroups.original_hs_version, sharedgroups.original_hs_code, sharedgroups.original_hs_suffix FROM commlistwithid INNER JOIN sharedgroups ON commlistwithid.commodity_id=sharedgroups.id_single and sharedgroups.commodity_id = 1037) v where v.share_group_commodity_id = 1037;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

//        sb.append("SELECT commodity_id, hs_code, hs_suffix, hs_version, short_description, shared_group_code FROM commlistwithid where commodity_id IN ("+commodity_id_list+")");
        sb.append(" SELECT * from (SELECT sharedgroups.commodity_id as shared_group_commodity_id, commlistwithid.commodity_id, commlistwithid.hs_code, commlistwithid.hs_suffix, commlistwithid.hs_version, commlistwithid.short_description, commlistwithid.description, sharedgroups.original_hs_version, sharedgroups.original_hs_code, sharedgroups.original_hs_suffix FROM commlistwithid INNER JOIN sharedgroups ON commlistwithid.commodity_id=sharedgroups.id_single and sharedgroups.commodity_id IN ( "+commodity_id+")) v where v.shared_group_commodity_id IN ( "+commodity_id+")");
        System.out.println("getCommodityInfo_commodity_id "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns commodity_id, target_code, description associated to the commodity id
    public JDBCIterablePolicy getCommodityInfo(DatasourceBean dsBean, String commodity_id_list) throws Exception{
        //  System.out.println("getCommodityInfo ");

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        //Through the commodity id get the shared_group_code
        //sb.append("SELECT commodity_id, target_code, description FROM commlistwithid where commodity_id IN ("+commodity_id_list+")");

        sb.append("SELECT hs_code, hs_suffix, hs_version, short_description FROM commlistwithid where commodity_id IN ("+commodity_id_list+")");
          System.out.println("getCommodityInfo sb "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns commodity_id, target_code, description associated to the commodity id
    public JDBCIterablePolicy getCommodityInfoIgnoringAssociatedPolicy(DatasourceBean dsBean, String commodity_class_list, String country_list, boolean sharedGroup) throws Exception{
        //  System.out.println("getCommodityInfo ");

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, description, short_description, commodityclass_code, commodityclass_name, shared_group_code, country_code FROM commlistwithid where commodityclass_code IN ("+commodity_class_list+") and country_code IN ("+country_list+")");
        if(!sharedGroup){
            sb.append(" and hs_code<>'none'");
        }
        System.out.println("getCommodityInfoIgnoringAssociatedPolicy sb "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns commodity_id, hs_code, hs_suffix, hs_version,
    //target_code, description, short_description,
    // commodity_class_code, commodity_class_name, shared_group_code
    //associated to the commodity_class_code
    public JDBCIterablePolicy getCommodityInfoByCommodityClassCode(DatasourceBean dsBean, String commodityClassCode, String countryCode, String policymeasureCode) throws Exception{
        //  System.out.println("getCommodityInfo ");

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

//        sb.append("SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, target_code, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodityclass_code IN ("+commodityClassCode+")");
//        sb.append("SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, target_code, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodity_id  IN (");
        sb.append("SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodity_id  IN (");
//        sb.append("SELECT DISTINCT(commodity_id) from mastertable where commodityclass_code="+commodityClassCode+" and country_code="+countryCode+" and policymeasure_code="+policymeasureCode+")");
        sb.append("SELECT DISTINCT(commodity_id) from mastertable where commodityclass_code="+commodityClassCode+" ");
        if(countryCode!=null){
            sb.append(" and country_code="+countryCode+" ");
        }
        if(policymeasureCode!=null){
            sb.append(" and policymeasure_code="+policymeasureCode);
        }
        sb.append(")");
        System.out.println("getCommodityInfo sb "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns commodity_id, hs_code, hs_suffix, hs_version,
    //target_code, description, short_description,
    // commodity_class_code, commodity_class_name, shared_group_code
    //associated to the commodity_class_code
    public JDBCIterablePolicy getCommodityInfoWithUnionHS4HS6(DatasourceBean dsBean, String commodityDomainCode, String policyDomainCode, String commodityClassCode, String countryCode, Boolean withSharedGroupsBool) throws Exception{
        //  System.out.println("getCommodityInfo ");

        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

//        sb.append("SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, target_code, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodityclass_code IN ("+commodityClassCode+")");
//        sb.append("SELECT * from (SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, target_code, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodity_id  IN (");
        sb.append("SELECT * from (SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodity_id  IN (");
//        sb.append("SELECT DISTINCT(commodity_id) from mastertable where commodityclass_code="+commodityClassCode+" and country_code="+countryCode+" and policymeasure_code="+policymeasureCode+")");
        sb.append("SELECT DISTINCT(commodity_id) from mastertable where commodityclass_code IN ("+commodityClassCode+") ");
        if(countryCode!=null){
            sb.append(" and country_code="+countryCode+" ");
        }
        if(commodityDomainCode!=null){
            sb.append(" and commoditydomain_code IN ("+commodityDomainCode+") ");
        }
        if(policyDomainCode!=null){
            sb.append(" and policydomain_code IN ("+policyDomainCode+") ");
        }
        sb.append(") ");
        //UNION SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, target_code, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodity_id  IN (SELECT DISTINCT(commodity_id) from mastertable where commodityclass_code=3) and hs_code<>'none' and CAST (hs_code AS FLOAT) < 1000000;
//        sb.append("UNION SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, target_code, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodity_id  IN (SELECT DISTINCT(commodity_id) from mastertable where commodityclass_code="+commodityClassCode+") and hs_code<>'none' and CAST (hs_code AS FLOAT) < 1000000) AS policy order by policy.hs_code ASC");
        sb.append("UNION SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, description, short_description, commodityclass_code, commodityclass_name, shared_group_code FROM commlistwithid where commodity_id  IN (SELECT DISTINCT(commodity_id) from mastertable where commodityclass_code IN ("+commodityClassCode+")) and hs_code<>'none' and CAST (hs_code AS FLOAT) < 1000000) AS policy ");
        if(!withSharedGroupsBool){
            sb.append("where shared_group_code IS NULL ");
        }
        sb.append("order by policy.hs_code ASC");
        System.out.println("getCommodityInfo sb "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns all the policy fields using the cpl_id
    public JDBCIterablePolicy getPolicyFromCplId(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        //   System.out.println("getPolicyFromCplId start ");
//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        // sb.append("SELECT * from policytable where cpl_id IN ("+pd_obj.getCpl_id()+") AND");
        sb.append("SELECT "+getSelectForPolicyTable()+" from policytable, commlistwithid where cpl_id IN ("+pd_obj.getCpl_id()+") AND commlistwithid.commodity_id IN ("+pd_obj.getCommodity_id()+") AND policytable.commodity_id=commlistwithid.commodity_id ");
        if(pd_obj.getYearTab().equals("classic"))
        {
            if((pd_obj.getYear_list()!=null)&&(!pd_obj.getYear_list().isEmpty()))
            {
                sb.append("AND ((EXTRACT(year FROM start_date) IN ("+pd_obj.getYear_list()+")) OR (EXTRACT(year FROM end_date) IN ("+pd_obj.getYear_list()+"))) ");
            }
        }
        else
        {   //Format: 01/03/2014
            String start_date = pd_obj.getStart_date();
            String end_date = pd_obj.getEnd_date();
            //((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010'))))
            sb.append("AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"')  OR ((policytable.start_date <= '"+start_date+"') AND (policytable.end_date>='"+end_date+"')) OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//            sb.append("((policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"') OR (policytable.end_date >= '"+end_date+"' AND policytable.end_date <= '"+end_date+"') OR (policytable.end_date IS NULL AND (policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"')))");
        }
        //ORDER BY policy_element, start_date, exemptions
        sb.append(" ORDER BY policy_element, start_date, exemptions ASC");
          System.out.println("getPolicyFromCplId  policy preview "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns all the policy fields using the cpl_id
    public JDBCIterablePolicy getPolicyListFromCplId(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
//        SELECT metadata_id, policy_id, cpl_id, commlistwithid.commodity_id, hs_version, hs_code, hs_suffix, policy_element, EXTRACT(day FROM start_date) || '/' || EXTRACT(month FROM start_date) || '/' || EXTRACT(year FROM start_date), EXTRACT(day FROM end_date) || '/' || EXTRACT(month FROM end_date) || '/' || EXTRACT(year FROM end_date), units, value, value_text, value_type, exemptions, location_condition, notes, link, source, title_of_notice, legal_basis_name, EXTRACT(day FROM date_of_publication) || '/' || EXTRACT(month FROM date_of_publication) || '/' || EXTRACT(year FROM date_of_publication) AS date_of_publication, imposed_end_date, second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link, original_dataset, type_of_change_code, type_of_change_name, measure_description, product_original_hs, product_original_name, link_pdf, benchmark_link_pdf, short_description, shared_group_code, description, date_of_publication as date_of_publication2 from policytableviewpd_and_startdate, commlistwithid where cpl_id IN (43) AND date_of_publication < '30/5/2008' AND policytableviewpd_and_startdate.commodity_id=commlistwithid.commodity_id  ORDER BY date_of_publication2 DESC
        //policytableviewpd_and_startdate
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT "+getSelectForPolicyTable()+", date_of_publication as date_of_publication2 from policytableviewpd_and_startdate, commlistwithid where cpl_id IN ("+pd_obj.getCpl_id()+")");
        if((pd_obj.getDate_of_publication()!=null)&&(pd_obj.getDate_of_publication().length()>0)){
            sb.append(" AND date_of_publication < '"+pd_obj.getDate_of_publication()+"'");
        }
        sb.append(" AND policytableviewpd_and_startdate.commodity_id=commlistwithid.commodity_id ");
        sb.append(" ORDER BY date_of_publication2 DESC");
        System.out.println("getPolicyListFromCplId "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //Select from policy table
//    public StringBuilder getSelectForPolicyTable(){
//        StringBuilder select = new StringBuilder();
////        select.append("sbPolicy.metadata_id, sbPolicy.policy_id, sbPolicy.cpl_id, mastertable.country_code, mastertable.country_name, mastertable.subnational_code, mastertable.subnational_name, mastertable.commoditydomain_code, mastertable.commoditydomain_name, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policydomain_code, mastertable.policydomain_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.condition, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.commodity_description, sbPolicy.shared_group_code, sbPolicy.location_condition, sbPolicy.start_date, sbPolicy.end_date, sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.exemptions, sbPolicy.value_calculated, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, sbPolicy.measure_descr, sbPolicy.short_description, sbPolicy.original_dataset, sbPolicy.type_of_change_name, sbPolicy.type_of_change_code, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.policy_element, sbPolicy.impl, sbPolicy.second_generation_specific, sbPolicy.imposed_end_date, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.source_benchmark, sbPolicy.date_of_publication, sbPolicy.xs_yeartype, sbPolicy.notes_datepub");
//        select.append("metadata_id, policy_id, cpl_id, commodity_id, hs_version, hs_code, hs_suffix, commodity_description, shared_group_code, location_condition, ");
//        select.append(" EXTRACT(day FROM start_date) || '-' || EXTRACT(month FROM start_date) || '-' || EXTRACT(year FROM start_date), EXTRACT(day FROM end_date) || '-' || EXTRACT(month FROM end_date) || '-' || EXTRACT(year FROM end_date), ");
//        select.append(" units, value, value_text, exemptions, value_calculated, notes, link, source, title_of_notice, legal_basis_name, measure_descr, short_description, original_dataset, type_of_change_name, ");
//        select.append(" type_of_change_code, product_original_hs, product_original_name, policy_element, impl, second_generation_specific, imposed_end_date, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, ");
//        select.append(" start_date_tax, source_benchmark, date_of_publication, xs_yeartype, notes_datepub");
//        return select;
//    }

    public StringBuilder getSelectForPolicyTable(){
        StringBuilder select = new StringBuilder();
        //SELECT metadata_id, policy_id, cpl_id, commlistwithid.commodity_id, hs_version, hs_code, hs_suffix, policy_element, EXTRACT(day FROM start_date) || '-' || EXTRACT(month FROM start_date) || '-' || EXTRACT(year FROM start_date), EXTRACT(day FROM end_date) || '-' || EXTRACT(month FROM end_date) || '-' || EXTRACT(year FROM end_date), units, value, value_text, value_type, exemptions, location_condition, notes, link, source, title_of_notice, legal_basis_name, date_of_publication, imposed_end_date, second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link, original_dataset, type_of_change_code, type_of_change_name, measure_descr, product_original_hs, product_original_name, implementationprocedure, xs_yeartype, link_pdf, benchmark_link_pdf from policytable, commlistwithid where cpl_id IN (1) AND commlistwithid.commodity_id IN (108) AND ((policytable.start_date BETWEEN '2006-02-11' AND '2011-02-11') OR (policytable.end_date BETWEEN '2006-02-11' AND '2011-02-11') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '2006-02-11' AND '2011-02-11') OR (policytable.start_date < '2006-02-11'))));
        select.append("metadata_id, policy_id, cpl_id, commlistwithid.commodity_id, hs_version, hs_code, hs_suffix, policy_element, ");
        select.append("EXTRACT(day FROM start_date) || '/' || EXTRACT(month FROM start_date) || '/' || EXTRACT(year FROM start_date), EXTRACT(day FROM end_date) || '/' || EXTRACT(month FROM end_date) || '/' || EXTRACT(year FROM end_date), ");
        select.append("units, value, value_text, value_type, exemptions, location_condition, notes, link, source, title_of_notice, legal_basis_name, EXTRACT(day FROM date_of_publication) || '/' || EXTRACT(month FROM date_of_publication) || '/' || EXTRACT(year FROM date_of_publication) AS date_of_publication, imposed_end_date, ");
        select.append("second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link, original_dataset, type_of_change_code, type_of_change_name, measure_description, product_original_hs, product_original_name, ");
        select.append("link_pdf, benchmark_link_pdf, short_description, shared_group_code, description ");
        return select;
    }

    public JDBCIterablePolicy getDownloadExport(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{

//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        JDBCIterablePolicy it = new JDBCIterablePolicy();

        //Query to have the distinct on the cpl_id based on the user selection
        //StringBuilder sbCplId = getDistinctCplId_fromMaster(pd_obj);
        StringBuilder total = new StringBuilder();
        boolean with_commodity_id = true;
        JDBCIterablePolicy itCplId = getDownloadPreview(dsBean, pd_obj, with_commodity_id);
        String s_cpl_id="";
        String s_commodity_id="";
        //To avoid duplicates
        List<String> commodity_id_list = new ArrayList<String>();
        while(itCplId.hasNext()) {
            //From [299, 320] to 299, 320
            List<String> valueList = itCplId.next();
            // System.out.println("valueList "+valueList.toString());
            //There is only one value
            String value = valueList.toString();
            int last_index = value.lastIndexOf("]");
            value = value.substring(1, last_index);
            value = value.replaceAll("\\s+","");
            //From 299,320
            last_index = value.indexOf(",");
            //299
            String cpl_id = value.substring(0, last_index);
            String commodity_id = value.substring(last_index+1);
            s_cpl_id+= cpl_id;
            boolean duplicate_not_found = false;
            if(!commodity_id_list.contains(commodity_id))
            {
                if((s_commodity_id!=null)&&(s_commodity_id.length()>0))
                {
                    //In this way it will be inserted once
                    s_commodity_id+=",";
                }
                s_commodity_id+= commodity_id;
                commodity_id_list.add(commodity_id);
                duplicate_not_found = true;
            }
            //  System.out.println("value "+value+" new value "+s);
            if(itCplId.hasNext())
            {
                s_cpl_id+=",";
//                if(duplicate_not_found)
//                {
//
//                }
            }
        }
        //No cpl_id has available for this selection
        if(s_cpl_id.length()==0)
        {
            return new JDBCIterablePolicy();
        }

        //Query to have the selection on the policy based on the cpl_id, commodity_id and the time
        StringBuilder sbPolicyWithTime = getStringQueryPolicyFromCplIdTime(pd_obj, s_cpl_id, s_commodity_id);

        /*SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
    FROM Orders
    INNER JOIN Customers
    ON Orders.CustomerID=Customers.CustomerID;*/

        total.append("SELECT "+getSelectForJoinMasterPolicyTable_fromMaster()+" FROM ("+sbPolicyWithTime.toString()+") sbPolicy JOIN mastertable ON sbPolicy.cpl_id = mastertable.cpl_id ORDER BY sbPolicy.policy_element, sbPolicy.start_date, sbPolicy.exemptions ASC");
         System.out.println("getDownloadExport "+total.toString());
        it.query(dsBean, total.toString());
        return it;
    }

//    public Map<String, LinkedList<String>> getDownloadShareGroupExport(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
//       // System.out.println("getDownloadShareGroupExport start ");
//        Map<String, LinkedList<String>> map1 = new LinkedHashMap<String, LinkedList<String>>();
//
////      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
////      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
//       // JDBCIterablePolicy it = new JDBCIterablePolicy();
//        //Query to have the distinct on the cpl_id based on the user selection
//        //StringBuilder sbCplId = getDistinctCplId_fromMaster(pd_obj);
//
//        //1-Getting all the cpli_id for the user selection
//        StringBuilder total = new StringBuilder();
//        boolean with_commodity_id = false;
//        JDBCIterablePolicy itCplId = getDownloadPreview(dsBean, pd_obj, with_commodity_id);
//        String s1="";
//        while(itCplId.hasNext()) {
//            //From [289] to 289
//            List<String> valueList = itCplId.next();
//            //  System.out.println("valueList "+valueList.toString());
//            //There is only one value
//            String value = valueList.toString();
//            s1+= value.substring(1,(value.length()-1));
//            //  System.out.println("value "+value+" new value "+s);
//            if (itCplId.hasNext())
//                s1+=",";
//        }
//        //No cpl_id has available for this selection
//        if(s1.length()==0)
//        {
//            //return new JDBCIterablePolicy();
//            return map1;
//        }
//
//        //2-Getting the commodity id associated with each cpli_id
//        //select distinct (commodity_id) from mastertable where cpl_id ='1';
//        JDBCIterablePolicy itCommodityId = getSelectCommodityIdFromCplId_fromMaster(dsBean, s1);
//        if(itCommodityId.hasNext()==false)
//        {
//            //return new JDBCIterablePolicy();
//          //  System.out.println("if(itCommodityId.hasNext()==false)");
//            return map1;
//        }
//
//        //3-For each commodity id check if it's a share group
//        String commodity_id="";
//        while(itCommodityId.hasNext())
//        {
//            //System.out.println("while(itCommodityId.hasNext())");
//            List<String> valueListCplId = itCommodityId.next();
//            //System.out.println("valueListCplId "+valueListCplId);
//            String valueCplId = valueListCplId.toString();
//            //System.out.println("valueCplId " + valueCplId);
//            commodity_id = valueCplId.substring(1,(valueCplId.length()-1));
//           // System.out.println("Commodity id "+commodity_id);
//            //For each commodity id
//
//            final JDBCIterablePolicy it =  getshareGroupInfo(dsBean, commodity_id);
//            //it.closeConnection();
//
//            while(it.hasNext()) {
//                //Get the share group code associated to the commodity_id
//                String valueshare_group_code = (it.next()).toString();
//                //System.out.println("valueshare_group_code "+valueshare_group_code);
//                String share_group_code = valueshare_group_code.substring(1,(valueshare_group_code.length()-1));
//               // System.out.println(" share_group_code "+share_group_code);
//
//                if((share_group_code!=null)&&(!share_group_code.equals("none")&&(!share_group_code.isEmpty())))
//                {
//                    //Get the list of commodity that belongs to this share_group_code
//                    final JDBCIterablePolicy it2 = getSingleIdFromCommodityId(dsBean, commodity_id);
//                    //Adding also info about the share group
//                    String s = ""+commodity_id+",";
//                    while(it2.hasNext()) {
//                        //From [289] to 289
//                        List<String> valueList = it2.next();
//                        //  System.out.println("valueList "+valueList.toString());
//                        //There is only one value
//                        String value = valueList.toString();
//    //                    value = value.substring(2, value.lastIndexOf("\""));
//    //                   s+= "'"+g.toJson(it2.next())+"'";
//    //                    s+= "'"+it2.next()+"'";
//
//                        s+= value.substring(1,(value.length()-1));
//                        //  System.out.println("value "+value+" new value "+s);
//                        if (it2.hasNext())
//                            s+=",";
//                    }
//                    it2.closeConnection();
//                    //  System.out.println("s = "+s);
//                    if(s.length()==0)
//                    {
//                        //  System.out.println("(s.length()==0)");
//                        //It is not a share group
//                        //return new JDBCIterablePolicy();
//                       // System.out.println("No commodity associated with that share group if(s.length()==0)");
//                        continue;
////                        return map1;
//                    }
//                    else
//                    {
//                       // System.out.println("FOUND!!!!!");
//                        //  System.out.println("(s.length()!0)");
//                        //Get commodity_id, target_code, description associated with the commodityId
////                        final JDBCIterablePolicy it3 = getCommodityInfo(dsBean, s);
////                        String commodity_id_value="";
////                        while(it3.hasNext()) {
////                            //From [289] to 289
////                            List<String> valueList = it3.next();
////                          //  System.out.println("after getCommodityInfo valueList "+valueList.toString());
////                            //commodity_id, target_code, description
////                            commodity_id_value = valueList.get(0).toString();
//////                            commodity_id_value = value.substring(1,(value.length()-1));
////                            String target_code = "";
////                            String description = "";
////                            if(!map1.containsKey(commodity_id_value))
////                            {
////                                //This key is not in the map
////                                //Adding..
////                                LinkedList<String> list= new LinkedList<String>();
////                                target_code = valueList.get(1).toString();
////                                //System.out.println(" targ "+target_code);
////                                list.add(0, target_code);
////                                description = valueList.get(2).toString();
////                                //System.out.println(" descr "+description);
////                                list.add(1, description);
////                             //   System.out.println("PUTTING IN MAP  id = "+commodity_id_value+" target_code = "+target_code+" description = "+description);
////                                map1.put(commodity_id_value,list);
////                            }
////                        }
//
//                        final JDBCIterablePolicy it3 = getCommodityInfo_commodity_id(dsBean, s);
//                        String commodity_id_value="";
//                        while(it3.hasNext()) {
//                            //From [289] to 289
//                            List<String> valueList = it3.next();
//                            //  System.out.println("after getCommodityInfo valueList "+valueList.toString());
//                            //hs_code, hs_suffix, hs_version, short_description
//                            commodity_id_value = valueList.get(0).toString();
//                          //  System.out.println("commodity_id_value "+commodity_id_value);
////                            hs_code = value.substring(1,(value.length()-1));
//                            String hs_code="";
//                            String hs_suffix = "";
//                            String hs_version = "";
//                            String short_description = "";
//                            String shared_group_code = "";
//                            if(!map1.containsKey(commodity_id_value))
//                            {
//                                //This key is not in the map
//                                //Adding..
//                                LinkedList<String> list= new LinkedList<String>();
//                                if(valueList.get(1)!=null)
//                                {
//                                    hs_code = valueList.get(1).toString();
//                                }
//                                if(valueList.get(2)!=null)
//                                {
//                                    hs_suffix = valueList.get(2).toString();
//                                }
//                                if(valueList.get(3)!=null)
//                                {
//                                    hs_version = valueList.get(3).toString();
//                                }
//                                if(valueList.get(4)!=null)
//                                {
//                                    short_description = valueList.get(4).toString();
//                                }
//                                if(valueList.get(5)!=null)
//                                {
//                                    shared_group_code = valueList.get(5).toString();
//                                }
//
//                               // System.out.println(" hs_code "+hs_code);
//                                list.add(0, hs_code);
//                              //  System.out.println(" hs_suffix "+hs_suffix);
//                                list.add(1, hs_suffix);
//                              //  System.out.println(" hs_version "+hs_version);
//                                list.add(2, hs_version);
//                             //   System.out.println(" short_description "+short_description);
//                                list.add(3, short_description);
//                                list.add(4, commodity_id_value);
//                                list.add(5, shared_group_code);
//                             //   System.out.println("PUTTING IN MAP  id = "+hs_code+" hs_suffix = "+hs_suffix+" hs_version = "+hs_version +" short_description "+short_description);
//                                map1.put(commodity_id_value,list);
//                            }
//                        }
//                        it3.closeConnection();
//                        // writer.write(g.toJson(it3.next()));
//                      //  return map1;
//    //                    return it3;
//                    }
//                }
//                else{
//                    //It is not a share group
//                    // Initiate the stream
//                    //return new JDBCIterablePolicy();
//                  //  System.out.println("else continue");
//                    continue;
//                    //return map1;
//                }
//            }
//            it.closeConnection();
//        }//For each commodity id
//
//        //To change
//        return map1;
//    }

    public Map<String, Map<String, LinkedList<String>>> getDownloadShareGroupExport(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        // System.out.println("getDownloadShareGroupExport start ");
        // Map<String, LinkedList<String>> map1 = new LinkedHashMap<String, LinkedList<String>>();
        //First key share group commodity id....
        //Second key commodity Id of each commodity that belong to that share group
        //the list contains the field of that commodity
        Map<String, Map<String, LinkedList<String>>> map1 = new LinkedHashMap<String, Map<String, LinkedList<String>>>();

//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        // JDBCIterablePolicy it = new JDBCIterablePolicy();
        //Query to have the distinct on the cpl_id based on the user selection
        //StringBuilder sbCplId = getDistinctCplId_fromMaster(pd_obj);

        //1-Getting all the cpli_id for the user selection
        StringBuilder total = new StringBuilder();
        boolean with_commodity_id = false;
        JDBCIterablePolicy itCplId = getDownloadPreview(dsBean, pd_obj, with_commodity_id);
        String s1="";
        while(itCplId.hasNext()) {
            //From [289] to 289
            List<String> valueList = itCplId.next();
            //  System.out.println("valueList "+valueList.toString());
            //There is only one value
            String value = valueList.toString();
            s1+= value.substring(1,(value.length()-1));
            //  System.out.println("value "+value+" new value "+s);
            if (itCplId.hasNext())
                s1+=",";
        }
        //No cpl_id has available for this selection
        if(s1.length()==0)
        {
            //return new JDBCIterablePolicy();
            return map1;
        }

        //2-Getting the commodity id associated with each cpli_id
        //select distinct (commodity_id) from mastertable where cpl_id ='1';
        JDBCIterablePolicy itCommodityId = getSelectCommodityIdFromCplId_fromMaster(dsBean, s1);
        if(itCommodityId.hasNext()==false)
        {
            //return new JDBCIterablePolicy();
            //  System.out.println("if(itCommodityId.hasNext()==false)");
            return map1;
        }

        //3-For each commodity id check if it's a share group
        String commodity_id="";
        while(itCommodityId.hasNext())
        {
            //System.out.println("while(itCommodityId.hasNext())");
            List<String> valueListCplId = itCommodityId.next();
            //System.out.println("valueListCplId "+valueListCplId);
            String valueCplId = valueListCplId.toString();
            //System.out.println("valueCplId " + valueCplId);
            commodity_id = valueCplId.substring(1,(valueCplId.length()-1));
            // System.out.println("Commodity id "+commodity_id);
            //For each commodity id
            final JDBCIterablePolicy it =  getshareGroupInfo(dsBean, commodity_id);
            //it.closeConnection();

            while(it.hasNext()) {
                //Get the share group code associated to the commodity_id
                String valueshare_group_code = (it.next()).toString();
                //System.out.println("valueshare_group_code "+valueshare_group_code);
                String share_group_code = valueshare_group_code.substring(1,(valueshare_group_code.length()-1));
                // System.out.println(" share_group_code "+share_group_code);

                if((share_group_code!=null)&&(!share_group_code.equals("none")&&(!share_group_code.isEmpty())))
                {
                    //Get the list of commodity that belongs to this share_group_code
                    final JDBCIterablePolicy it2 = getSingleIdFromCommodityId(dsBean, commodity_id);
                    //Adding also info about the share group
                    String s = ""+commodity_id+",";
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
                    it2.closeConnection();
                    //  System.out.println("s = "+s);
                    if(s.length()==0)
                    {
                        //No commodity associated with that share group
                        // System.out.println("No commodity associated with that share group if(s.length()==0)");
                        continue;
//                        return map1;
                    }
                    else
                    {
                        //commodity_id contains the id of the share group
                        //The result is:
                        //share_group_commodity_id | commodity_id | hs_code  | hs_suffix | hs_version | short_description | original_hs_version | original_hs_code | original_hs_suffix
                        final JDBCIterablePolicy it3 = getCommodityInfo_commodity_id(dsBean, commodity_id);

                        String commodity_id_value="";
                        while(it3.hasNext()) {
                            List<String> valueList = it3.next();
                            //This is(commodity_id_value) the commodity id of each commodity that belong to the share group
                            commodity_id_value = valueList.get(1).toString();
                            if((commodity_id_value!=null)&&(commodity_id_value.length()!=0))
                            {
                                String shared_group_code = "";
                                String hs_code = "";
                                String hs_suffix = "";
                                String hs_version = "";
                                String short_description = "";
                                String original_hs_version = "";
                                String original_hs_code = "";
                                String original_hs_suffix = "";

                                //If the map of the specific share group doesn't contain the commodity id
                                if((!map1.containsKey(commodity_id))||(!map1.get(commodity_id).containsKey(commodity_id_value)))
                                {
                                    //Creation of the list for the new commodity
                                    LinkedList<String> list= new LinkedList<String>();
                                    shared_group_code = share_group_code;
                                    if(valueList.get(2)!=null)
                                    {
                                        hs_code = valueList.get(2).toString();
                                    }
                                    if(valueList.get(3)!=null)
                                    {
                                        hs_suffix = valueList.get(3).toString();
                                    }
                                    if(valueList.get(4)!=null)
                                    {
                                        hs_version = valueList.get(4).toString();
                                    }
                                    if(valueList.get(5)!=null)
                                    {
                                        short_description = valueList.get(5).toString();
                                    }
                                    if(valueList.get(6)!=null)
                                    {
                                        original_hs_version = valueList.get(6).toString();
                                    }
                                    if(valueList.get(7)!=null)
                                    {
                                        original_hs_code = valueList.get(7).toString();
                                    }
                                    if(valueList.get(8)!=null)
                                    {
                                        original_hs_suffix = valueList.get(8).toString();
                                    }
                                    list.add(0, shared_group_code);
                                    list.add(1, hs_code);
                                    list.add(2, hs_suffix);
                                    list.add(3, hs_version);
                                    list.add(4, short_description);
                                    list.add(5, original_hs_version);
                                    list.add(6, original_hs_code);
                                    list.add(7, original_hs_suffix);
                                    Map<String, LinkedList<String>> mp2 = new LinkedHashMap<String, LinkedList<String>>();
                                    mp2.put(commodity_id_value, list);

                                    if(!map1.containsKey(commodity_id))
                                    {
                                        //if the shared group was not in the map
                                        map1.put(commodity_id, mp2);
                                    }
                                    else
                                    {
                                        //get the map of the particular share group
                                        map1.get(commodity_id).put(commodity_id_value, list);
                                    }
                                }
                            }
                        }
                        it3.closeConnection();
                        // writer.write(g.toJson(it3.next()));
                        //  return map1;
                        //                    return it3;
                    }
                }
                else{
                    //It is not a share group
                    // Initiate the stream
                    //return new JDBCIterablePolicy();
                    //  System.out.println("else continue");
                    continue;
                    //return map1;
                }
            }
            it.closeConnection();
        }//For each commodity id

        //To change
        return map1;
    }

    public Map<String, Map<String, LinkedList<String>>> getSharedGroupInfoByCommodityList(DatasourceBean dsBean, POLICYDataObject pd_obj){

        System.out.println("getSharedGroupInfoByCommodityList start ");
        String commodityString = pd_obj.getCommodity_id();
        String[] commodityArray = commodityString.split(",");
        System.out.println(commodityArray);
        // System.out.println("getDownloadShareGroupExport start ");
        // Map<String, LinkedList<String>> map1 = new LinkedHashMap<String, LinkedList<String>>();
        //First key share group commodity id....
        //Second key commodity Id of each commodity that belong to that share group
        //the list contains the field of that commodity
        Map<String, Map<String, LinkedList<String>>> map1 = new LinkedHashMap<String, Map<String, LinkedList<String>>>();

//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        // JDBCIterablePolicy it = new JDBCIterablePolicy();
        //Query to have the distinct on the cpl_id based on the user selection
        //StringBuilder sbCplId = getDistinctCplId_fromMaster(pd_obj);

        //3-For each commodity id check if it's a share group
        String commodity_id="";
        for(int i=0; i<commodityArray.length; i++)
        {
            commodity_id = commodityArray[i];

            // System.out.println("Commodity id "+commodity_id);
            //For each commodity id
            try {
                final JDBCIterablePolicy it = getshareGroupInfo(dsBean, commodity_id);

                while(it.hasNext()) {
                    //Get the share group code associated to the commodity_id
                    String valueshare_group_code = (it.next()).toString();
                    //System.out.println("valueshare_group_code "+valueshare_group_code);
                    String share_group_code = valueshare_group_code.substring(1,(valueshare_group_code.length()-1));
                    // System.out.println(" share_group_code "+share_group_code);

                    if((share_group_code!=null)&&(!share_group_code.equals("none")&&(!share_group_code.isEmpty())))
                    {
                        //Get the list of commodity that belongs to this share_group_code
                        final JDBCIterablePolicy it2 = getSingleIdFromCommodityId(dsBean, commodity_id);
                        //Adding also info about the share group
                        String s = ""+commodity_id+",";
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
                        it2.closeConnection();
                        //  System.out.println("s = "+s);
                        if(s.length()==0)
                        {
                            //No commodity associated with that share group
                            // System.out.println("No commodity associated with that share group if(s.length()==0)");
                            continue;
//                        return map1;
                        }
                        else
                        {
                            //commodity_id contains the id of the share group
                            //The result is:
                            //share_group_commodity_id | commodity_id | hs_code  | hs_suffix | hs_version | short_description | original_hs_version | original_hs_code | original_hs_suffix
                            final JDBCIterablePolicy it3 = getCommodityInfo_commodity_idWithLongDescription(dsBean, commodity_id);

                            String commodity_id_value="";
                            while(it3.hasNext()) {
                                List<String> valueList = it3.next();
                                //This is(commodity_id_value) the commodity id of each commodity that belong to the share group
                                commodity_id_value = valueList.get(1).toString();
                                if((commodity_id_value!=null)&&(commodity_id_value.length()!=0))
                                {
                                    String shared_group_code = "";
                                    String hs_code = "";
                                    String hs_suffix = "";
                                    String hs_version = "";
                                    String short_description = "";
                                    String original_hs_version = "";
                                    String original_hs_code = "";
                                    String original_hs_suffix = "";

                                    //If the map of the specific share group doesn't contain the commodity id
                                    if((!map1.containsKey(commodity_id))||(!map1.get(commodity_id).containsKey(commodity_id_value)))
                                    {
                                        //Creation of the list for the new commodity
                                        LinkedList<String> list= new LinkedList<String>();
                                        shared_group_code = share_group_code;
                                        if(valueList.get(2)!=null)
                                        {
                                            hs_code = valueList.get(2).toString();
                                        }
                                        if(valueList.get(3)!=null)
                                        {
                                            hs_suffix = valueList.get(3).toString();
                                        }
                                        if(valueList.get(4)!=null)
                                        {
                                            hs_version = valueList.get(4).toString();
                                        }
                                        if(valueList.get(5)!=null)
                                        {
                                            short_description = valueList.get(5).toString();
                                        }
                                        if(valueList.get(6)!=null)
                                        {
                                            original_hs_version = valueList.get(6).toString();
                                        }
                                        if(valueList.get(7)!=null)
                                        {
                                            original_hs_code = valueList.get(7).toString();
                                        }
                                        if(valueList.get(8)!=null)
                                        {
                                            original_hs_suffix = valueList.get(8).toString();
                                        }
                                        list.add(0, shared_group_code);
                                        list.add(1, hs_code);
                                        list.add(2, hs_suffix);
                                        list.add(3, hs_version);
                                        list.add(4, short_description);
                                        list.add(5, original_hs_version);
                                        list.add(6, original_hs_code);
                                        list.add(7, original_hs_suffix);
                                        System.out.println("getSharedGroupInfoByCommodityList shared_group_code "+shared_group_code);
                                        System.out.println("getSharedGroupInfoByCommodityList hs_code "+hs_code);
                                        System.out.println("getSharedGroupInfoByCommodityList hs_suffix "+hs_suffix);
                                        System.out.println("getSharedGroupInfoByCommodityList hs_version "+hs_version);
                                        System.out.println("getSharedGroupInfoByCommodityList short_description "+short_description);
                                        System.out.println("getSharedGroupInfoByCommodityList original_hs_version "+original_hs_version);
                                        System.out.println("getSharedGroupInfoByCommodityList original_hs_code "+original_hs_code);
                                        System.out.println("getSharedGroupInfoByCommodityList original_hs_suffix "+original_hs_suffix);
                                        Map<String, LinkedList<String>> mp2 = new LinkedHashMap<String, LinkedList<String>>();
                                        mp2.put(commodity_id_value, list);

                                        if(!map1.containsKey(commodity_id))
                                        {
                                            //if the shared group was not in the map
                                            map1.put(commodity_id, mp2);
                                        }
                                        else
                                        {
                                            //get the map of the particular share group
                                            map1.get(commodity_id).put(commodity_id_value, list);
                                        }
                                    }
                                }
                            }
                            it3.closeConnection();
                            // writer.write(g.toJson(it3.next()));
                            //  return map1;
                            //                    return it3;
                        }
                    }
                    else{
                        //It is not a share group
                        // Initiate the stream
                        //return new JDBCIterablePolicy();
                        //  System.out.println("else continue");
                        continue;
                        //return map1;
                    }
                }
                it.closeConnection();
            }
            catch (Exception e ){
                System.out.println("Shared Group Info Exception "+e);
            }
            //it.closeConnection();

        }//For each commodity id

        System.out.println("getSharedGroupInfoByCommodityList end ");
        //To change
        return map1;
    }


    //Select form join the master and the policy based on the cplid
    public JDBCIterablePolicy getSelectCommodityIdFromCplId_fromMaster(DatasourceBean dsBean, String cplId_string) throws Exception{
        StringBuilder s = new StringBuilder();
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        s.append("select distinct(commodity_id) from mastertable where cpl_id IN ("+cplId_string+") order by commodity_id ASC");
        // System.out.println("getSelectCommodityIdFromCplId_fromMaster "+s.toString());
        it.query(dsBean, s.toString());
        return it;
    }

    //Select form join the master and the policy based on the cplid
    public StringBuilder getSelectForJoinMasterPolicyTable_fromMaster(){
        StringBuilder select = new StringBuilder();
        //SELECT sbPolicy.metadata_id, sbPolicy.policy_id, mastertable.cpl_id, mastertable.cpl_code, mastertable.country_code, mastertable.country_name, mastertable.subnational_code, mastertable.subnational_name, mastertable.commoditydomain_code, mastertable.commoditydomain_name, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policydomain_code, mastertable.policydomain_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.condition_code, mastertable.condition, mastertable.individualpolicy_code, mastertable.individualpolicy_name, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.short_description, sbPolicy.shared_group_code, sbPolicy.policy_element, EXTRACT(day FROM sbPolicy.start_date) || '-' || EXTRACT(month FROM sbPolicy.start_date) || '-' || EXTRACT(year FROM sbPolicy.start_date), EXTRACT(day FROM sbPolicy.end_date) || '-' || EXTRACT(month FROM sbPolicy.end_date) || '-' || EXTRACT(year FROM sbPolicy.end_date), sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.value_type, sbPolicy.exemptions, sbPolicy.location_condition, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, sbPolicy.date_of_publication, sbPolicy.imposed_end_date, sbPolicy.second_generation_specific, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.benchmark_link, sbPolicy.original_dataset, sbPolicy.type_of_change_code, sbPolicy.type_of_change_name, sbPolicy.measure_descr, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.implementationprocedure, sbPolicy.xs_yeartype, sbPolicy.link_pdf, sbPolicy.benchmark_link_pdf
//        select.append("sbPolicy.metadata_id, sbPolicy.policy_id, sbPolicy.cpl_id, mastertable.country_code, mastertable.country_name, mastertable.subnational_code, mastertable.subnational_name, mastertable.commoditydomain_code, mastertable.commoditydomain_name, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policydomain_code, mastertable.policydomain_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.condition, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.commodity_description, sbPolicy.shared_group_code, sbPolicy.location_condition, sbPolicy.start_date, sbPolicy.end_date, sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.exemptions, sbPolicy.value_calculated, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, sbPolicy.measure_descr, sbPolicy.short_description, sbPolicy.original_dataset, sbPolicy.type_of_change_name, sbPolicy.type_of_change_code, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.policy_element, sbPolicy.impl, sbPolicy.second_generation_specific, sbPolicy.imposed_end_date, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.source_benchmark, sbPolicy.date_of_publication, sbPolicy.xs_yeartype, sbPolicy.notes_datepub");
        //  select.append("sbPolicy.metadata_id, sbPolicy.policy_id, sbPolicy.cpl_id, mastertable.country_code, mastertable.country_name, mastertable.subnational_code, mastertable.subnational_name, mastertable.commoditydomain_code, mastertable.commoditydomain_name, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policydomain_code, mastertable.policydomain_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.condition, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.commodity_description, sbPolicy.shared_group_code, sbPolicy.location_condition, EXTRACT(day FROM sbPolicy.start_date) || '-' || EXTRACT(month FROM sbPolicy.start_date) || '-' || EXTRACT(year FROM sbPolicy.start_date), EXTRACT(day FROM sbPolicy.end_date) || '-' || EXTRACT(month FROM sbPolicy.end_date) || '-' || EXTRACT(year FROM sbPolicy.end_date), sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.exemptions, sbPolicy.value_calculated, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, sbPolicy.measure_descr, sbPolicy.short_description, sbPolicy.original_dataset, sbPolicy.type_of_change_name, sbPolicy.type_of_change_code, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.policy_element, sbPolicy.impl, sbPolicy.second_generation_specific, sbPolicy.imposed_end_date, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.source_benchmark, sbPolicy.date_of_publication, sbPolicy.xs_yeartype, sbPolicy.notes_datepub");
//With Metadata id
//        select.append("sbPolicy.metadata_id, sbPolicy.policy_id, mastertable.cpl_id, mastertable.cpl_code, mastertable.country_code, mastertable.country_name, mastertable.subnational_code, mastertable.subnational_name, mastertable.commoditydomain_code, mastertable.commoditydomain_name, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policydomain_code, mastertable.policydomain_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.condition_code, mastertable.condition, mastertable.individualpolicy_code, mastertable.individualpolicy_name, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.short_description, sbPolicy.shared_group_code, sbPolicy.policy_element, EXTRACT(day FROM sbPolicy.start_date) || '/' || EXTRACT(month FROM sbPolicy.start_date) || '/' || EXTRACT(year FROM sbPolicy.start_date), EXTRACT(day FROM sbPolicy.end_date) || '/' || EXTRACT(month FROM sbPolicy.end_date) || '/' || EXTRACT(year FROM sbPolicy.end_date), sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.value_type, sbPolicy.exemptions, sbPolicy.location_condition, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, EXTRACT(day FROM sbPolicy.date_of_publication) || '/' || EXTRACT(month FROM sbPolicy.date_of_publication) || '/' || EXTRACT(year FROM sbPolicy.date_of_publication), imposed_end_date, sbPolicy.second_generation_specific, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.benchmark_link, sbPolicy.original_dataset, sbPolicy.type_of_change_code, sbPolicy.type_of_change_name, sbPolicy.measure_descr, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.implementationprocedure, sbPolicy.xs_yeartype, sbPolicy.link_pdf, sbPolicy.benchmark_link_pdf ");
//Without Metadata id
        //Before removing policy_id
//        select.append("sbPolicy.policy_id, mastertable.cpl_id, mastertable.cpl_code, mastertable.country_code, mastertable.country_name, mastertable.subnational_code, mastertable.subnational_name, mastertable.commoditydomain_code, mastertable.commoditydomain_name, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policydomain_code, mastertable.policydomain_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.condition_code, mastertable.condition, mastertable.individualpolicy_code, mastertable.individualpolicy_name, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.short_description, sbPolicy.shared_group_code, sbPolicy.policy_element, EXTRACT(day FROM sbPolicy.start_date) || '/' || EXTRACT(month FROM sbPolicy.start_date) || '/' || EXTRACT(year FROM sbPolicy.start_date), EXTRACT(day FROM sbPolicy.end_date) || '/' || EXTRACT(month FROM sbPolicy.end_date) || '/' || EXTRACT(year FROM sbPolicy.end_date), sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.value_type, sbPolicy.exemptions, sbPolicy.location_condition, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, EXTRACT(day FROM sbPolicy.date_of_publication) || '/' || EXTRACT(month FROM sbPolicy.date_of_publication) || '/' || EXTRACT(year FROM sbPolicy.date_of_publication), imposed_end_date, sbPolicy.second_generation_specific, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.benchmark_link, sbPolicy.original_dataset, sbPolicy.type_of_change_code, sbPolicy.type_of_change_name, sbPolicy.measure_descr, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.implementationprocedure, sbPolicy.xs_yeartype, sbPolicy.link_pdf, sbPolicy.benchmark_link_pdf ");
        //After removing policy_id
        //select.append("mastertable.cpl_id, mastertable.cpl_code, mastertable.country_code, mastertable.country_name, mastertable.subnational_code, mastertable.subnational_name, mastertable.commoditydomain_code, mastertable.commoditydomain_name, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policydomain_code, mastertable.policydomain_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.condition_code, mastertable.condition, mastertable.individualpolicy_code, mastertable.individualpolicy_name, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.short_description, sbPolicy.shared_group_code, sbPolicy.policy_element, EXTRACT(day FROM sbPolicy.start_date) || '/' || EXTRACT(month FROM sbPolicy.start_date) || '/' || EXTRACT(year FROM sbPolicy.start_date), EXTRACT(day FROM sbPolicy.end_date) || '/' || EXTRACT(month FROM sbPolicy.end_date) || '/' || EXTRACT(year FROM sbPolicy.end_date), sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.value_type, sbPolicy.exemptions, sbPolicy.location_condition, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, EXTRACT(day FROM sbPolicy.date_of_publication) || '/' || EXTRACT(month FROM sbPolicy.date_of_publication) || '/' || EXTRACT(year FROM sbPolicy.date_of_publication), imposed_end_date, sbPolicy.second_generation_specific, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.benchmark_link, sbPolicy.original_dataset, sbPolicy.type_of_change_code, sbPolicy.type_of_change_name, sbPolicy.measure_descr, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.implementationprocedure, sbPolicy.xs_yeartype, sbPolicy.link_pdf, sbPolicy.benchmark_link_pdf ");
//
//        ·         cpl_code
//
//        ·         country_code
//
//        ·         subnational_code
//
//        ·         commoditydomain_code
//
//        ·         commodityclass_code
//
//        ·         policydomain_code
//
//        ·         policytype_code
//
//        ·         policymeasure_code
//
//        ·         condition_code
//
//        ·         individualpolicy_code
//
//        ·         type_of_change_code

        //New order
//        select.append("sbPolicy.policy_id, mastertable.cpl_id, mastertable.country_name, mastertable.subnational_name, mastertable.commoditydomain_name, mastertable.commodityclass_name, mastertable.policydomain_name, mastertable.policytype_name, mastertable.policymeasure_name, mastertable.condition, mastertable.individualpolicy_name, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.short_description, sbPolicy.description, sbPolicy.shared_group_code, sbPolicy.policy_element, EXTRACT(day FROM sbPolicy.start_date) || '/' || EXTRACT(month FROM sbPolicy.start_date) || '/' || EXTRACT(year FROM sbPolicy.start_date), EXTRACT(day FROM sbPolicy.end_date) || '/' || EXTRACT(month FROM sbPolicy.end_date) || '/' || EXTRACT(year FROM sbPolicy.end_date), sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.value_type, sbPolicy.exemptions, sbPolicy.location_condition, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, EXTRACT(day FROM sbPolicy.date_of_publication) || '/' || EXTRACT(month FROM sbPolicy.date_of_publication) || '/' || EXTRACT(year FROM sbPolicy.date_of_publication), imposed_end_date, sbPolicy.second_generation_specific, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.benchmark_link, sbPolicy.original_dataset, sbPolicy.type_of_change_name, sbPolicy.measure_descr, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.implementationprocedure, sbPolicy.xs_yeartype, sbPolicy.link_pdf, sbPolicy.benchmark_link_pdf,mastertable.cpl_code, mastertable.country_code, mastertable.subnational_code, mastertable.commoditydomain_code, mastertable.commodityclass_code, mastertable.policydomain_code, mastertable.policytype_code, mastertable.policymeasure_code, mastertable.condition_code, mastertable.individualpolicy_code, sbPolicy.type_of_change_code");
        select.append("sbPolicy.policy_id, mastertable.cpl_id, mastertable.country_name, mastertable.subnational_name, mastertable.commoditydomain_name, mastertable.commodityclass_name, mastertable.policydomain_name, mastertable.policytype_name, mastertable.policymeasure_name, mastertable.condition, mastertable.individualpolicy_name, sbPolicy.commodity_id, sbPolicy.hs_version, sbPolicy.hs_code, sbPolicy.hs_suffix, sbPolicy.short_description, sbPolicy.description, sbPolicy.shared_group_code, sbPolicy.policy_element, EXTRACT(day FROM sbPolicy.start_date) || '/' || EXTRACT(month FROM sbPolicy.start_date) || '/' || EXTRACT(year FROM sbPolicy.start_date), EXTRACT(day FROM sbPolicy.end_date) || '/' || EXTRACT(month FROM sbPolicy.end_date) || '/' || EXTRACT(year FROM sbPolicy.end_date), sbPolicy.units, sbPolicy.value, sbPolicy.value_text, sbPolicy.value_type, sbPolicy.exemptions, sbPolicy.location_condition, sbPolicy.notes, sbPolicy.link, sbPolicy.source, sbPolicy.title_of_notice, sbPolicy.legal_basis_name, EXTRACT(day FROM sbPolicy.date_of_publication) || '/' || EXTRACT(month FROM sbPolicy.date_of_publication) || '/' || EXTRACT(year FROM sbPolicy.date_of_publication), imposed_end_date, sbPolicy.second_generation_specific, sbPolicy.benchmark_tax, sbPolicy.benchmark_product, sbPolicy.tax_rate_biofuel, sbPolicy.tax_rate_benchmark, sbPolicy.start_date_tax, sbPolicy.benchmark_link, sbPolicy.original_dataset, sbPolicy.type_of_change_name, sbPolicy.measure_description, sbPolicy.product_original_hs, sbPolicy.product_original_name, sbPolicy.link_pdf, sbPolicy.benchmark_link_pdf, mastertable.country_code, mastertable.subnational_code, mastertable.commoditydomain_code, mastertable.commodityclass_code, mastertable.policydomain_code, mastertable.policytype_code, mastertable.policymeasure_code, mastertable.condition_code, mastertable.individualpolicy_code, sbPolicy.type_of_change_code");
        return select;
    }

    public StringBuilder getStringQueryPolicyFromCplIdTime(POLICYDataObject pd_obj, String sbCplId, String sCommodityId) throws Exception{
        //   System.out.println("getPolicyFromCplId start ");
//      select DISTINCT(mastertable.cpl_id) from mastertable, policytable where mastertable.cpl_id=policytable.cpl_id AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR
//      (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010')))) AND commoditydomain_code IN () AND policydomain_code IN () AND commodityclass_code IN () AND policytype_code IN () AND policymeasure_code IN () AND country_code IN () ORDER BY mastertable.cpl_id;
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT policytable.metadata_id, policytable.policy_id, policytable.cpl_id, policytable.commodity_id, commlistwithid.hs_code, commlistwithid.hs_suffix, commlistwithid.hs_version, commlistwithid.short_description, commlistwithid.description, commlistwithid.shared_group_code, policytable.policy_element, policytable.start_date, policytable.end_date, policytable.units, policytable.value, policytable.value_text, policytable.value_type, policytable.exemptions, policytable.location_condition, policytable.notes, policytable.link, policytable.source, policytable.title_of_notice, policytable.legal_basis_name, policytable.date_of_publication, policytable.imposed_end_date, policytable.second_generation_specific, policytable.benchmark_tax, policytable.benchmark_product, policytable.tax_rate_biofuel, policytable.tax_rate_benchmark, policytable.start_date_tax, policytable.benchmark_link, policytable.original_dataset, policytable.type_of_change_code, policytable.type_of_change_name, policytable.measure_description, policytable.product_original_hs, policytable.product_original_name, policytable.link_pdf, policytable.benchmark_link_pdf ");
        sb.append("from policytable, commlistwithid where cpl_id IN ("+sbCplId+") AND commlistwithid.commodity_id IN ("+sCommodityId+") AND policytable.commodity_id=commlistwithid.commodity_id ");
        if(pd_obj.getYearTab().equals("classic"))
        {
            if((pd_obj.getYear_list()!=null)&&(!pd_obj.getYear_list().isEmpty()))
            {
                sb.append("AND ((EXTRACT(year FROM start_date) IN ("+pd_obj.getYear_list()+")) OR (EXTRACT(year FROM end_date) IN ("+pd_obj.getYear_list()+")))");
            }
        }
        else
        {   //Format: 01/03/2014
            String start_date = pd_obj.getStart_date();
            String end_date = pd_obj.getEnd_date();
            //((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010'))))
            sb.append("AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"')  OR ((policytable.start_date <= '"+start_date+"') AND (policytable.end_date>='"+end_date+"')) OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//            sb.append("((policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"') OR (policytable.end_date >= '"+end_date+"' AND policytable.end_date <= '"+end_date+"') OR (policytable.end_date IS NULL AND (policytable.start_date >= '"+start_date+"' AND policytable.start_date <= '"+start_date+"')))");
        }
        //   System.out.println("getStringQueryPolicyFromCplIdTime sb "+sb.toString());
        return sb;
    }
//Original
//    public Map<String, LinkedHashMap<String, String>> biofuelPolicies_timeSeries(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
//        // System.out.println("biofuelPolicies_timeSeries start ");
//        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
//        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
//        for(int i=0; i<policy_type_code_array.length; i++)
//        {
//            String single_policy_type = policy_type_code_array[i];
//            System.out.println("START single_policy_type "+single_policy_type);
//            StringBuilder timeSeries_query = getStringQueryBiofuelPolicyTypes_TimeSeries(pd_obj, single_policy_type);
//
//            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
//            it.query(dsBean, timeSeries_query.toString());
//            //it.closeConnection();
//            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
////            if(single_policy_type.equals("8")){
////            System.out.println("START biofuelPolicies_timeSeries !!!!!!");}
//            while(it.hasNext()) {
//                //[2006-01-01, India]
//                //System.out.println(it.next());
//                String time_country = it.next().toString();
//                String time = time_country.substring(1, time_country.indexOf(','));
//                // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time );
//                if(countryCount_map.containsKey(time))
//                {
//                    String country_cont= countryCount_map.get(time);
//                    Integer count = Integer.parseInt(country_cont);
//                    count++;
//                    countryCount_map.put(time, ""+count);
//                    if(single_policy_type.equals("8")){
//                    System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time+" count "+countryCount_map.get(time) );
//                    }
//                    //countryCount_map.get(time).
//                }
//                else
//                {
//                    countryCount_map.put(time, "1");
//
//                }
//                // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time+" count "+countryCount_map.get(time) );
//            }
////            if(single_policy_type.equals("8")){
////                System.out.println("end biofuelPolicies_timeSeries !!!!!!");
////            }
//
//            pt_map.put(policy_type_code_array[i], countryCount_map);
//            System.out.println("end policy type "+single_policy_type);
//            Set<String> set = countryCount_map.keySet();
//            for(String elem : set){
//                System.out.println("elem = "+elem+" value = "+countryCount_map.get(elem));
//            }
//        }
//
//        return pt_map;
//    }

    //Backup
    public Map<String, LinkedHashMap<String, String>> biofuelPolicies_timeSeries(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        StringBuilder generate_query = getGenerateSeries(pd_obj);
        //System.out.println("generate_query= "+generate_query.toString());

        //This is used ONLY to check if the data belong to WTO
        String mesureCodeArray[]= pd_obj.getPolicy_measure_code();
        String policy_measure ="";
        for(int i=0; i< mesureCodeArray.length; i++){
            if(policy_measure.length()>0){
                policy_measure+= ",";
            }
            policy_measure+= mesureCodeArray[i];
        }
        for(int i=0; i<policy_type_code_array.length; i++)
        {
            final JDBCIterablePolicy it_generateSeries =  new JDBCIterablePolicy();
            it_generateSeries.query(dsBean, generate_query.toString());
            String single_policy_type = policy_type_code_array[i];
            //StringBuilder timeSeries_query = getStringQueryBiofuelPolicyTypes_TimeSeries(pd_obj, single_policy_type);
            StringBuilder timeSeries_query = getStringQueryBiofuelPolicyTypes_TimeSeriesWithoutWTO(pd_obj, single_policy_type, policy_measure);

            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
            LinkedHashMap<String, LinkedList<String>> countryList_map = new LinkedHashMap<String, LinkedList<String>>();
            while(it_generateSeries.hasNext()) {
                String days = it_generateSeries.next().toString();
                days = days.substring(1,days.length()-1);
                countryCount_map.put(days, "0");
                countryList_map.put(days, new LinkedList<String>());
            }
            LinkedList<String []> timeSeriesList = new LinkedList<String[]>();
            it.query(dsBean, timeSeries_query.toString());
            while(it.hasNext()) {
                //[2006-01-01, India]
                //[India, 2006-01-01, 2010-01-01]
                String time_country = it.next().toString();
                //Removing []
                time_country = time_country.substring(1, time_country.length() - 1);
                //String  to array
                String[] result = time_country.split(", ");

                String country = result[0];
                String start_date = result[1];
                String end_date = result[2];
                //System.out.println(end_date);
                String timeseriesArray[] = {country,start_date,end_date};
                timeSeriesList.add(timeseriesArray);
            }

            Set<String> keySet = countryCount_map.keySet();
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            for(String key: keySet){
                Date date = null;
                try {
                    date = df.parse(key);
                    for(int iEl=0; iEl<timeSeriesList.size(); iEl++){
                        String array[]= timeSeriesList.get(iEl);
                        //Array = country,start_date,end_date
                        Date start_date_from_query = null;
                        Date end_date_from_query = null;

                        start_date_from_query = df.parse(array[1]);
                        end_date_from_query = df.parse(array[2]);

                        //Compare the date
                        if((date.compareTo(start_date_from_query)>=0) && (date.compareTo(end_date_from_query)<=0)){
                            if(!countryList_map.get(key).contains(array[0])){
//                                System.out.println("array[0] ");
//                                System.out.println(array[0]);
                                countryList_map.get(key).add(array[0]);
                                int count = Integer.parseInt(countryCount_map.get(key));
                                int countPlus = count+1;
//                                System.out.println("key= "+key+ " "+"countPlus= "+countPlus);
                                countryCount_map.put(key, ""+countPlus);
                            }
                        }
                    }
//                    System.out.println("key= "+key+ " "+"countPlus= "+countryCount_map.get(key));
//                    System.out.println("countryList_map.get(key)");
//                    System.out.println(countryList_map.get(key));
                } catch (ParseException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
//            System.out.println("policy_type_code_array[i]= "+policy_type_code_array[i]+ "");
//            System.out.println(policy_type_code_array);
            pt_map.put(policy_type_code_array[i], countryCount_map);
        }

        return pt_map;
    }

    public Map<String, LinkedHashMap<String, String>>[] biofuelPolicies_timeSeriesExcel(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        StringBuilder generate_query = getGenerateSeries(pd_obj);
        //System.out.println("generate_query= "+generate_query.toString());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String start_date_from_client = pd_obj.getStart_date();
        String end_date_from_client = pd_obj.getEnd_date();
        Date start_date_from_client_date = df.parse(start_date_from_client);
        Date end_date_from_client_date = df.parse(end_date_from_client);
        Calendar cal_start_date = new GregorianCalendar();
        cal_start_date.setTime(start_date_from_client_date);
        Calendar cal_end_date = new GregorianCalendar();
        cal_end_date.setTime(end_date_from_client_date);
//        System.out.println("biofuelPolicies_timeSeriesExcel start_date_from_client END DATE before parse 1!!!!");
//        System.out.println(start_date_from_client);
//        System.out.println("biofuelPolicies_timeSeriesExcel start_date_from_client_date END DATE 2!!!!");
//        System.out.println(start_date_from_client_date);
//
//        System.out.println("biofuelPolicies_timeSeriesExcel end_date_from_client END DATE before parse 1!!!!");
//        System.out.println(end_date_from_client);
//        System.out.println("biofuelPolicies_timeSeriesExcel end_date_from_client_date END DATE 2!!!!");
//        System.out.println(end_date_from_client_date);

        //This is used ONLY to check if the data belong to WTO
        String mesureCodeArray[]= pd_obj.getPolicy_measure_code();
        String policy_measure ="";
        for(int i=0; i< mesureCodeArray.length; i++){
            if(policy_measure.length()>0){
                policy_measure+= ",";
            }
            policy_measure+= mesureCodeArray[i];
        }

        Map<String, LinkedHashMap<String, String>> excel_map_array[] = new LinkedHashMap[policy_type_code_array.length];
//        System.out.println("policy_type_code_array");
//        System.out.println(policy_type_code_array.length);
//        for(int i=0; i<policy_type_code_array.length; i++) {
//            System.out.println(policy_type_code_array[i]);
//        }
        for(int i=0; i<policy_type_code_array.length; i++)
        {
            Map<String, LinkedHashMap<String, String>> excel_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
            final JDBCIterablePolicy it_generateSeries =  new JDBCIterablePolicy();
            it_generateSeries.query(dsBean, generate_query.toString());
            String single_policy_type = policy_type_code_array[i];
            //StringBuilder timeSeries_query = getStringQueryBiofuelPolicyTypes_TimeSeries(pd_obj, single_policy_type);
            StringBuilder timeSeries_query = getStringQueryBiofuelPolicyTypes_TimeSeriesWithoutWTO(pd_obj, single_policy_type, policy_measure);

            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
            LinkedHashMap<String, LinkedList<String>> countryList_map = new LinkedHashMap<String, LinkedList<String>>();
            while(it_generateSeries.hasNext()) {
                String days = it_generateSeries.next().toString();
                days = days.substring(1,days.length()-1);
                countryCount_map.put(days, "0");
                countryList_map.put(days, new LinkedList<String>());
            }
            LinkedList<String []> timeSeriesList = new LinkedList<String[]>();
            it.query(dsBean, timeSeries_query.toString());
            while(it.hasNext()) {
                //[2006-01-01, India]
                //[India, 2006-01-01, 2010-01-01]
                String time_country = it.next().toString();
                //Removing []
                time_country = time_country.substring(1, time_country.length() - 1);
                //String  to array
                String[] result = time_country.split(", ");

                String country = result[0];
                String start_date = result[1];
                String end_date = result[2];
                //System.out.println(end_date);
                String timeseriesArray[] = {country,start_date,end_date};
                timeSeriesList.add(timeseriesArray);
            }

            Set<String> keySet = countryCount_map.keySet();

            int previus_month = -1;
            int previus_year = -1;
            for(String key: keySet){
                LinkedHashMap<String, String> countryMap = new LinkedHashMap<String, String>();
                countryMap.put("PolicyType", policy_type_code_array[i]);

                Date date = null;
                try {
                    date = df.parse(key);
                    Calendar cal = new GregorianCalendar();
                    cal.setTime(date);
                    int month = cal.get(Calendar.MONTH)+1;
                    int year = cal.get(Calendar.YEAR);
                    if(previus_year!=year){
                        previus_month = -1;
                        previus_year=year;
                    }

                    //System.out.println("previus_month = "+previus_month);
                    //System.out.println("month = "+month);
                    if(previus_month != month){
                        if((cal.compareTo(cal_start_date)>0)&&(cal.compareTo(cal_end_date)<=0))
                        {
                            String monthAlpha = month_numToAlpha(month);
//                        String time = ""+month+""+year;
                            String time = ""+monthAlpha+" "+year;
                            for(int iEl=0; iEl<timeSeriesList.size(); iEl++){
                                String array[]= timeSeriesList.get(iEl);
                                //Array = country,start_date,end_date
                                Date start_date_from_query = null;
                                Date end_date_from_query = null;

                                start_date_from_query = df.parse(array[1]);
                                end_date_from_query = df.parse(array[2]);

                                //Compare the date
                                if((date.compareTo(start_date_from_query)>=0) && (date.compareTo(end_date_from_query)<=0)){
                                    if(!countryList_map.get(key).contains(array[0])){
                                        countryList_map.get(key).add(array[0]);
                                        int count = Integer.parseInt(countryCount_map.get(key));
                                        int countPlus = count+1;
                                        countryCount_map.put(key, ""+countPlus);
                                    }
                                }
                            }

                            countryMap.put("SUM", ""+countryCount_map.get(key));
                            countryMap.put("Argentina", "0");
                            countryMap.put("Australia", "0");
                            countryMap.put("Brazil", "0");
                            countryMap.put("Canada", "0");
                            countryMap.put("China", "0");
                            countryMap.put("Egypt", "0");
                            countryMap.put("European Union", "0");
                            countryMap.put("France", "0");
                            countryMap.put("Germany", "0");
                            countryMap.put("India", "0");
                            countryMap.put("Indonesia", "0");
                            countryMap.put("Italy", "0");
                            countryMap.put("Japan", "0");
                            countryMap.put("Kazakhstan", "0");
                            countryMap.put("Mexico", "0");
                            countryMap.put("Nigeria", "0");
                            countryMap.put("Philippines", "0");
                            countryMap.put("Republic of Korea", "0");
                            countryMap.put("Russian Federation", "0");
                            countryMap.put("Saudi Arabia", "0");
                            countryMap.put("South Africa", "0");
                            countryMap.put("Spain", "0");
                            countryMap.put("Thailand", "0");
                            countryMap.put("Turkey", "0");
                            countryMap.put("U.K. of Great Britain and Northern Ireland", "0");
                            countryMap.put("Ukraine", "0");
                            countryMap.put("United States of America", "0");
                            countryMap.put("Viet Nam", "0");

                            countryList_map.get(key);
                            LinkedList<String> countryArray = countryList_map.get(key);
//                        System.out.println("countryArray");
//                        System.out.println(countryArray);
//
//                        System.out.println(countryMap.get("Argentina"));
//                        System.out.println(countryMap.get("Australia"));
//                        System.out.println(countryMap.get("Brazil"));
//                        System.out.println(countryMap.get("Canada"));
//                        System.out.println(countryMap.get("China"));
//                        System.out.println(countryMap.get("Egypt"));
//                        System.out.println(countryMap.get("European Union"));
//                        System.out.println(countryMap.get("France"));
//                        System.out.println(countryMap.get("Germany"));
//                        System.out.println(countryMap.get("India"));
//                        System.out.println(countryMap.get("Indonesia"));
//                        System.out.println(countryMap.get("Italy"));
//                        System.out.println(countryMap.get("Japan"));
//                        System.out.println(countryMap.get("Kazakhstan"));
//                        System.out.println(countryMap.get("Mexico"));
//                        System.out.println(countryMap.get("Nigeria"));
//                        System.out.println(countryMap.get("Philippines"));
//                        System.out.println(countryMap.get("Republic of Korea"));
//                        System.out.println(countryMap.get("Russian Federation"));
//                        System.out.println(countryMap.get("Saudi Arabia"));
//                        System.out.println(countryMap.get("South Africa"));
//                        System.out.println(countryMap.get("Spain"));
//                        System.out.println(countryMap.get("Thailand"));
//                        System.out.println(countryMap.get("Turkey"));
//                        System.out.println(countryMap.get("U.K. of Great Britain and Northern Ireland"));
//                        System.out.println(countryMap.get("Ukraine"));
//                        System.out.println(countryMap.get("United States of America"));
//                        System.out.println(countryMap.get("Viet Nam"));

                            for(int iCountry =0; iCountry<countryArray.size(); iCountry++){
                                String countryName = countryArray.get(iCountry);
                                countryMap.put(countryName, "1");
                            }

                            //System.out.println("INSIDE!!! ");
                            excel_map.put(time, countryMap);
                            previus_month = month;
                            //System.out.println("Key= "+ key);
                            //System.out.println("PolicyType= "+excel_map.get(time).get("PolicyType"));
                            //System.out.println("SUM= "+excel_map.get(time).get("SUM"));
//                        System.out.println("ARGENTINA= "+excel_map.get(time).get("Argentina"));
//                        System.out.println("AUSTRALIA= "+excel_map.get(time).get("Australia"));
//                        System.out.println("BRAZIL= "+excel_map.get(time).get("Brazil"));
//                        System.out.println("CANADA= "+excel_map.get(time).get("Canada"));
//                        System.out.println("CHINA= "+excel_map.get(time).get("China"));
//                        System.out.println("EGYPT= "+excel_map.get(time).get("Egypt"));
                        }
                    }

//                    System.out.println("key= "+key+ " "+"countPlus= "+countryCount_map.get(key));
//                    System.out.println("countryList_map.get(key)");
//                    System.out.println(countryList_map.get(key));
                } catch (ParseException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
//            System.out.println("policy_type_code_array[i]= "+policy_type_code_array[i]+ "");
//            System.out.println(policy_type_code_array);
            pt_map.put(policy_type_code_array[i], countryCount_map);

            excel_map_array[i]=excel_map;
        }

        return excel_map_array;
//        return pt_map;
    }

    public String month_numToAlpha(int month){
        String ris = "Jan";

        switch(month){
            case 1:
                ris= "Jan";
                break;
            case 2:
                ris= "Feb";
                break;
            case 3:
                ris= "Mar";
                break;
            case 4:
                ris= "Apr";
                break;
            case 5:
                ris= "May";
                break;
            case 6:
                ris= "Jun";
                break;
            case 7:
                ris= "Jul";
                break;
            case 8:
                ris= "Ago";
                break;
            case 9:
                ris= "Sep";
                break;
            case 10:
                ris= "Oct";
                break;
            case 11:
                ris= "Nov";
                break;
            case 12:
                ris= "Dec";
                break;
            default:
                ris= "Jan";
        }
        return ris;
    }


    //Original
//    public Map<String, LinkedHashMap<String, String>> biofuelPolicyMeasures_timeSeries(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
//        // System.out.println("biofuelPolicies_timeSeries start ");
//        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
//        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
//        StringBuilder generate_query = getGenerateSeries(pd_obj);
//
//        String single_policy_type = policy_type_code_array[0];
//        String policy_meaure_code_array[]= pd_obj.getPolicy_measure_code();
//        for(int i=0; i<policy_meaure_code_array.length; i++)
//        {
//            String single_policy_measure = policy_meaure_code_array[i];
//            StringBuilder timeSeries_query = getStringQueryBiofuelPolicyMeasures_TimeSeries(pd_obj, single_policy_type, single_policy_measure);
//
//            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
//            it.query(dsBean, timeSeries_query.toString());
//            //it.closeConnection();
//            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
//            while(it.hasNext()) {
//                //[2006-01-01, India]
//                //System.out.println(it.next());
//                String time_country = it.next().toString();
//                String time = time_country.substring(1, time_country.indexOf(','));
//                // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time );
//                if(countryCount_map.containsKey(time))
//                {
//                    String country_cont= countryCount_map.get(time);
//                    Integer count = Integer.parseInt(country_cont);
//                    count++;
//                    countryCount_map.put(time, ""+count);
////                    System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time+" count "+countryCount_map.get(time) );
//                    //countryCount_map.get(time).
//                }
//                else
//                {
//                    countryCount_map.put(time, "1");
//
//                }
//                // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time+" count "+countryCount_map.get(time) );
//            }
//            pt_map.put(policy_meaure_code_array[i], countryCount_map);
//        }
//
//        Set<String> mapString = pt_map.keySet();
//        for(String key :mapString){
//            LinkedHashMap<String, String> list = pt_map.get(key);
//            Set<String> listString = list.keySet();
//            for(String elemlist: listString){
//                System.out.println("biofuelPolicyMeasures_timeSeries policy measure code key= "+key+" time elemlist= "+elemlist+ " count value = "+list.get(elemlist));
//            }
//        }
//        return pt_map;
//    }

    public Map<String, LinkedHashMap<String, String>> biofuelPolicyMeasures_timeSeries(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        // System.out.println("biofuelPolicies_timeSeries start ");
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        StringBuilder generate_query = getGenerateSeries(pd_obj);

        String single_policy_type = policy_type_code_array[0];
        String policy_meaure_code_array[]= pd_obj.getPolicy_measure_code();
        for(int i=0; i<policy_meaure_code_array.length; i++)
        {
            final JDBCIterablePolicy it_generateSeries =  new JDBCIterablePolicy();
            it_generateSeries.query(dsBean, generate_query.toString());
            String single_policy_measure = policy_meaure_code_array[i];
            StringBuilder timeSeries_query = getStringQueryBiofuelPolicyMeasures_TimeSeries(pd_obj, single_policy_type, single_policy_measure);

            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
            LinkedHashMap<String, LinkedList<String>> countryList_map = new LinkedHashMap<String, LinkedList<String>>();
            while(it_generateSeries.hasNext()) {
                String days = it_generateSeries.next().toString();
//                System.out.println("days");
//                System.out.println(days);
                days = days.substring(1,days.length()-1);
                countryCount_map.put(days, "0");
                countryList_map.put(days, new LinkedList<String>());
            }

            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
            LinkedList<String []> timeSeriesList = new LinkedList<String[]>();
            it.query(dsBean, timeSeries_query.toString());

            while(it.hasNext()) {
                //[2006-01-01, India]
                //[India, 2006-01-01, 2010-01-01]
                String time_country = it.next().toString();
                //Removing []
                time_country = time_country.substring(1, time_country.length() - 1);
                //String  to array
                String[] result = time_country.split(", ");

                String country = result[0];
                String start_date = result[1];
                String end_date = result[2];
                String timeseriesArray[] = {country,start_date,end_date};
                timeSeriesList.add(timeseriesArray);
            }

            Set<String> keySet = countryCount_map.keySet();
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            for(String key: keySet){
//                System.out.println("KEY!!!");
//                System.out.println(key);
                Date date = null;
                try {
                    date = df.parse(key);
                    for(int iEl=0; iEl<timeSeriesList.size(); iEl++){
                        String array[]= timeSeriesList.get(iEl);
                        //Array = country,start_date,end_date
                        Date start_date_from_query = null;
                        Date end_date_from_query = null;

                        start_date_from_query = df.parse(array[1]);
                        end_date_from_query = df.parse(array[2]);

                        //Compare the date
                        if((date.compareTo(start_date_from_query)>=0) && (date.compareTo(end_date_from_query)<=0)){
                            if(!countryList_map.get(key).contains(array[0])){
                                countryList_map.get(key).add(array[0]);
                                int count = Integer.parseInt(countryCount_map.get(key));
                                int countPlus = count+1;
                                countryCount_map.put(key, ""+countPlus);
                            }
                        }
                    }
                } catch (ParseException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
            pt_map.put(policy_meaure_code_array[i], countryCount_map);
        }

        Set<String> mapString = pt_map.keySet();
//        for(String key :mapString){
//            LinkedHashMap<String, String> list = pt_map.get(key);
//            Set<String> listString = list.keySet();
//            for(String elemlist: listString){
//                System.out.println("biofuelPolicyMeasures_timeSeries policy measure code key= "+key+" time elemlist= "+elemlist+ " count value = "+list.get(elemlist));
//            }
//        }
        return pt_map;
    }

    public Map<String, LinkedHashMap<String, String>>[][] biofuelPolicyMeasures_timeSeriesExcel(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        // System.out.println("biofuelPolicies_timeSeries start ");
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        //String policy_type_measure_info_array[][]= pd_obj.getPolicyTypesMeasureInfo();
        LinkedHashMap<String, LinkedHashMap<String, String>> policy_type_measure_info_array= pd_obj.getPolicyTypesMeasureInfoName();
        StringBuilder generate_query = getGenerateSeries(pd_obj);

        //ADDED START
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String start_date_from_client = pd_obj.getStart_date();
        String end_date_from_client = pd_obj.getEnd_date();
        Date start_date_from_client_date = df.parse(start_date_from_client);
        Date end_date_from_client_date = df.parse(end_date_from_client);
        Calendar cal_start_date = new GregorianCalendar();
        cal_start_date.setTime(start_date_from_client_date);
        Calendar cal_end_date = new GregorianCalendar();
        cal_end_date.setTime(end_date_from_client_date);
        //ADDED END
//        System.out.println("biofuelPolicyMeasures_timeSeriesExcel END DATE!!!!");
//        System.out.println(end_date_from_client_date);

//        Map<String, LinkedHashMap<String, String>> excel_map_array_pt_pm[][] = new LinkedHashMap[policy_type_measure_info_array.length][];
        Map<String, LinkedHashMap<String, String>> excel_map_array_pt_pm[][] = new LinkedHashMap[policy_type_code_array.length][];
        for(int z=0; z<policy_type_code_array.length; z++)
        {
            String single_policy_type = policy_type_code_array[z];
//            String policy_meaure_code_array[]= pd_obj.getPolicy_measure_code();
            LinkedHashMap<String, String> pm_infoNameMap = policy_type_measure_info_array.get(single_policy_type);
            Set<String> pm_keySet= pm_infoNameMap.keySet();
            Iterator<String> it_pm = pm_keySet.iterator();
            String policy_meaure_code_array[]= new String[pm_keySet.size()];
            for(int x =0; x<policy_meaure_code_array.length; x++){
                policy_meaure_code_array[x] = it_pm.next();
            }

           // System.out.println("policy_meaure_code_array");
//        System.out.println(policy_meaure_code_array);
            Map<String, LinkedHashMap<String, String>> excel_map_array[] = new LinkedHashMap[policy_meaure_code_array.length];
            for(int i=0; i<policy_meaure_code_array.length; i++)
            {
               // System.out.println("i= "+i+"Measure code= "+policy_meaure_code_array[i]);
                Map<String, LinkedHashMap<String, String>> excel_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
                final JDBCIterablePolicy it_generateSeries =  new JDBCIterablePolicy();
                it_generateSeries.query(dsBean, generate_query.toString());
                String single_policy_measure = policy_meaure_code_array[i];
                StringBuilder timeSeries_query = getStringQueryBiofuelPolicyMeasures_TimeSeries(pd_obj, single_policy_type, single_policy_measure);

                LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
                LinkedHashMap<String, LinkedList<String>> countryList_map = new LinkedHashMap<String, LinkedList<String>>();
                while(it_generateSeries.hasNext()) {
                    String days = it_generateSeries.next().toString();
                    days = days.substring(1,days.length()-1);
                    countryCount_map.put(days, "0");
                    countryList_map.put(days, new LinkedList<String>());
                }

                final JDBCIterablePolicy it =  new JDBCIterablePolicy();
                LinkedList<String []> timeSeriesList = new LinkedList<String[]>();
                it.query(dsBean, timeSeries_query.toString());

                while(it.hasNext()) {
                    //[2006-01-01, India]
                    //[India, 2006-01-01, 2010-01-01]
                    String time_country = it.next().toString();
                    //Removing []
                    time_country = time_country.substring(1, time_country.length() - 1);
                    //String  to array
                    String[] result = time_country.split(", ");

                    String country = result[0];
                    String start_date = result[1];
                    String end_date = result[2];
                    String timeseriesArray[] = {country,start_date,end_date};
                    timeSeriesList.add(timeseriesArray);
                }

                Set<String> keySet = countryCount_map.keySet();

                int previus_month = -1;
                int previus_year = -1;
                //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                for(String key: keySet){
                    LinkedHashMap<String, String> countryMap = new LinkedHashMap<String, String>();
                    countryMap.put("PolicyMeasure", policy_meaure_code_array[i]);

                    Date date = null;
                    try {
                        date = df.parse(key);
                        Calendar cal = new GregorianCalendar();
                        cal.setTime(date);
                        int month = cal.get(Calendar.MONTH)+1;
                        int year = cal.get(Calendar.YEAR);
                        if(previus_year!=year){
                            previus_month = -1;
                            previus_year=year;
                        }

                        //System.out.println("previus_month = "+previus_month);
                        //System.out.println("month = "+month);
                        if(previus_month != month) {
                            if ((cal.compareTo(cal_start_date) > 0) && (cal.compareTo(cal_end_date) <= 0)) {
                                String monthAlpha = month_numToAlpha(month);
//                        String time = ""+month+""+year;
                                String time = "" + monthAlpha + " " + year;
                                for (int iEl = 0; iEl < timeSeriesList.size(); iEl++) {
                                    String array[] = timeSeriesList.get(iEl);
                                    //Array = country,start_date,end_date
                                    Date start_date_from_query = null;
                                    Date end_date_from_query = null;

                                    start_date_from_query = df.parse(array[1]);
                                    end_date_from_query = df.parse(array[2]);

                                    //Compare the date
                                    if ((date.compareTo(start_date_from_query) >= 0) && (date.compareTo(end_date_from_query) <= 0)) {
                                        if (!countryList_map.get(key).contains(array[0])) {
                                            countryList_map.get(key).add(array[0]);
                                            int count = Integer.parseInt(countryCount_map.get(key));
                                            int countPlus = count + 1;
                                            countryCount_map.put(key, "" + countPlus);
                                        }
                                    }
                                }
                                countryMap.put("SUM", ""+countryCount_map.get(key));
                                countryMap.put("Argentina", "0");
                                countryMap.put("Australia", "0");
                                countryMap.put("Brazil", "0");
                                countryMap.put("Canada", "0");
                                countryMap.put("China", "0");
                                countryMap.put("Egypt", "0");
                                countryMap.put("European Union", "0");
                                countryMap.put("France", "0");
                                countryMap.put("Germany", "0");
                                countryMap.put("India", "0");
                                countryMap.put("Indonesia", "0");
                                countryMap.put("Italy", "0");
                                countryMap.put("Japan", "0");
                                countryMap.put("Kazakhstan", "0");
                                countryMap.put("Mexico", "0");
                                countryMap.put("Nigeria", "0");
                                countryMap.put("Philippines", "0");
                                countryMap.put("Republic of Korea", "0");
                                countryMap.put("Russian Federation", "0");
                                countryMap.put("Saudi Arabia", "0");
                                countryMap.put("South Africa", "0");
                                countryMap.put("Spain", "0");
                                countryMap.put("Thailand", "0");
                                countryMap.put("Turkey", "0");
                                countryMap.put("U.K. of Great Britain and Northern Ireland", "0");
                                countryMap.put("Ukraine", "0");
                                countryMap.put("United States of America", "0");
                                countryMap.put("Viet Nam", "0");

                                countryList_map.get(key);
                                LinkedList<String> countryArray = countryList_map.get(key);
//                        System.out.println("countryArray");
//                        System.out.println(countryArray);
//
//                        System.out.println(countryMap.get("Argentina"));
//                        System.out.println(countryMap.get("Australia"));
//                        System.out.println(countryMap.get("Brazil"));
//                        System.out.println(countryMap.get("Canada"));
//                        System.out.println(countryMap.get("China"));
//                        System.out.println(countryMap.get("Egypt"));
//                        System.out.println(countryMap.get("European Union"));
//                        System.out.println(countryMap.get("France"));
//                        System.out.println(countryMap.get("Germany"));
//                        System.out.println(countryMap.get("India"));
//                        System.out.println(countryMap.get("Indonesia"));
//                        System.out.println(countryMap.get("Italy"));
//                        System.out.println(countryMap.get("Japan"));
//                        System.out.println(countryMap.get("Kazakhstan"));
//                        System.out.println(countryMap.get("Mexico"));
//                        System.out.println(countryMap.get("Nigeria"));
//                        System.out.println(countryMap.get("Philippines"));
//                        System.out.println(countryMap.get("Republic of Korea"));
//                        System.out.println(countryMap.get("Russian Federation"));
//                        System.out.println(countryMap.get("Saudi Arabia"));
//                        System.out.println(countryMap.get("South Africa"));
//                        System.out.println(countryMap.get("Spain"));
//                        System.out.println(countryMap.get("Thailand"));
//                        System.out.println(countryMap.get("Turkey"));
//                        System.out.println(countryMap.get("U.K. of Great Britain and Northern Ireland"));
//                        System.out.println(countryMap.get("Ukraine"));
//                        System.out.println(countryMap.get("United States of America"));
//                        System.out.println(countryMap.get("Viet Nam"));

                                for(int iCountry =0; iCountry<countryArray.size(); iCountry++){
                                    String countryName = countryArray.get(iCountry);
                                    countryMap.put(countryName, "1");
                                }

                                //System.out.println("INSIDE!!! ");
                                excel_map.put(time, countryMap);
                                previus_month = month;
//                                System.out.println("Key= "+ key);
//                                System.out.println("PolicyType= "+excel_map.get(time).get("PolicyType"));
//                                System.out.println("SUM= "+excel_map.get(time).get("SUM"));
//                        System.out.println("ARGENTINA= "+excel_map.get(time).get("Argentina"));
//                        System.out.println("AUSTRALIA= "+excel_map.get(time).get("Australia"));
//                        System.out.println("BRAZIL= "+excel_map.get(time).get("Brazil"));
//                        System.out.println("CANADA= "+excel_map.get(time).get("Canada"));
//                        System.out.println("CHINA= "+excel_map.get(time).get("China"));
//                        System.out.println("EGYPT= "+excel_map.get(time).get("Egypt"));
                            }
                        }
                        //                    System.out.println("key= "+key+ " "+"countPlus= "+countryCount_map.get(key));
//                    System.out.println("countryList_map.get(key)");
//                    System.out.println(countryList_map.get(key));
                    } catch (ParseException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
                pt_map.put(policy_meaure_code_array[i], countryCount_map);
                excel_map_array[i]=excel_map;
            }
            excel_map_array_pt_pm[z]= excel_map_array;
        }

//        Set<String> mapString = pt_map.keySet();
//        for(String key :mapString){
//            LinkedHashMap<String, String> list = pt_map.get(key);
//            Set<String> listString = list.keySet();
//            for(String elemlist: listString){
//                System.out.println("biofuelPolicyMeasures_timeSeries policy measure code key= "+key+" time elemlist= "+elemlist+ " count value = "+list.get(elemlist));
//            }
//        }
//        return pt_map;
        return excel_map_array_pt_pm;
    }

    public Map<String, LinkedHashMap<String, String>>[][] exportRestrictions_timeSeriesExcel(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        // System.out.println("biofuelPolicies_timeSeries start ");
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        //String policy_type_measure_info_array[][]= pd_obj.getPolicyTypesMeasureInfo();
        LinkedHashMap<String, LinkedHashMap<String, String>> policy_type_measure_info_array= pd_obj.getPolicyTypesMeasureInfoName();
        StringBuilder generate_query = getGenerateSeries(pd_obj);

        //ADDED START
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String start_date_from_client = pd_obj.getStart_date();
        String end_date_from_client = pd_obj.getEnd_date();
        Date start_date_from_client_date = df.parse(start_date_from_client);
        Date end_date_from_client_date = df.parse(end_date_from_client);
        Calendar cal_start_date = new GregorianCalendar();
        cal_start_date.setTime(start_date_from_client_date);
        Calendar cal_end_date = new GregorianCalendar();
        cal_end_date.setTime(end_date_from_client_date);
        //ADDED END

//        Map<String, LinkedHashMap<String, String>> excel_map_array_pt_pm[][] = new LinkedHashMap[policy_type_measure_info_array.length][];
        Map<String, LinkedHashMap<String, String>> excel_map_array_pt_pm[][] = new LinkedHashMap[policy_type_code_array.length][];
        for(int z=0; z<policy_type_code_array.length; z++)
        {
            String single_policy_type = policy_type_code_array[z];
//            String policy_meaure_code_array[]= pd_obj.getPolicy_measure_code();
            LinkedHashMap<String, String> pm_infoNameMap = policy_type_measure_info_array.get(single_policy_type);
            Set<String> pm_keySet= pm_infoNameMap.keySet();
            Iterator<String> it_pm = pm_keySet.iterator();
            String policy_meaure_code_array[]= new String[pm_keySet.size()];
            for(int x =0; x<policy_meaure_code_array.length; x++){
                policy_meaure_code_array[x] = it_pm.next();
            }

            //System.out.println("policy_meaure_code_array");
//        System.out.println(policy_meaure_code_array);
            Map<String, LinkedHashMap<String, String>> excel_map_array[] = new LinkedHashMap[policy_meaure_code_array.length];
            for(int i=0; i<policy_meaure_code_array.length; i++)
            {
               // System.out.println("i= "+i+"Measure code= "+policy_meaure_code_array[i]);
                Map<String, LinkedHashMap<String, String>> excel_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
                final JDBCIterablePolicy it_generateSeries =  new JDBCIterablePolicy();
                it_generateSeries.query(dsBean, generate_query.toString());
                String single_policy_measure = policy_meaure_code_array[i];
//                StringBuilder timeSeries_query = getStringQueryBiofuelPolicyMeasures_TimeSeries(pd_obj, single_policy_type, single_policy_measure);
                StringBuilder timeSeries_query = getStringQueryExportRestrictionsPolicyMeasures_TimeSeries(pd_obj, single_policy_type, single_policy_measure);

                LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
                LinkedHashMap<String, LinkedList<String>> countryList_map = new LinkedHashMap<String, LinkedList<String>>();
                while(it_generateSeries.hasNext()) {
                    String days = it_generateSeries.next().toString();
                    days = days.substring(1,days.length()-1);
                    countryCount_map.put(days, "0");
                    countryList_map.put(days, new LinkedList<String>());
                }

                final JDBCIterablePolicy it =  new JDBCIterablePolicy();
                LinkedList<String []> timeSeriesList = new LinkedList<String[]>();
                it.query(dsBean, timeSeries_query.toString());

                while(it.hasNext()) {
                    //[2006-01-01, India]
                    //[India, 2006-01-01, 2010-01-01]
                    String time_country = it.next().toString();
                    //Removing []
                    time_country = time_country.substring(1, time_country.length() - 1);
                    //String  to array
                    String[] result = time_country.split(", ");

                    String country = result[0];
                    String start_date = result[1];
                    String end_date = result[2];
                    String timeseriesArray[] = {country,start_date,end_date};
                    timeSeriesList.add(timeseriesArray);
                }

                Set<String> keySet = countryCount_map.keySet();

                int previus_month = -1;
                int previus_year = -1;
                //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                for(String key: keySet){
                    LinkedHashMap<String, String> countryMap = new LinkedHashMap<String, String>();
                    countryMap.put("PolicyMeasure", policy_meaure_code_array[i]);

                    Date date = null;
                    try {
                        date = df.parse(key);
                        Calendar cal = new GregorianCalendar();
                        cal.setTime(date);
                        int month = cal.get(Calendar.MONTH)+1;
                        int year = cal.get(Calendar.YEAR);
                        if(previus_year!=year){
                            previus_month = -1;
                            previus_year=year;
                        }

                        //System.out.println("previus_month = "+previus_month);
                        //System.out.println("month = "+month);
                        if(previus_month != month) {
                            if ((cal.compareTo(cal_start_date) > 0) && (cal.compareTo(cal_end_date) <= 0)) {
                                String monthAlpha = month_numToAlpha(month);
//                        String time = ""+month+""+year;
                                String time = "" + monthAlpha + " " + year;
                                for (int iEl = 0; iEl < timeSeriesList.size(); iEl++) {
                                    String array[] = timeSeriesList.get(iEl);
                                    //Array = country,start_date,end_date
                                    Date start_date_from_query = null;
                                    Date end_date_from_query = null;

                                    start_date_from_query = df.parse(array[1]);
                                    end_date_from_query = df.parse(array[2]);

                                    //Compare the date
                                    if ((date.compareTo(start_date_from_query) >= 0) && (date.compareTo(end_date_from_query) <= 0)) {
                                        if (!countryList_map.get(key).contains(array[0])) {
                                            countryList_map.get(key).add(array[0]);
                                            int count = Integer.parseInt(countryCount_map.get(key));
                                            int countPlus = count + 1;
                                            countryCount_map.put(key, "" + countPlus);
                                        }
                                    }
                                }
                                countryMap.put("SUM", ""+countryCount_map.get(key));
                                countryMap.put("Argentina", "0");
                                countryMap.put("Australia", "0");
                                countryMap.put("Brazil", "0");
                                countryMap.put("Canada", "0");
                                countryMap.put("China", "0");
                                countryMap.put("Egypt", "0");
                                countryMap.put("European Union", "0");
                                countryMap.put("France", "0");
                                countryMap.put("Germany", "0");
                                countryMap.put("India", "0");
                                countryMap.put("Indonesia", "0");
                                countryMap.put("Italy", "0");
                                countryMap.put("Japan", "0");
                                countryMap.put("Kazakhstan", "0");
                                countryMap.put("Mexico", "0");
                                countryMap.put("Nigeria", "0");
                                countryMap.put("Philippines", "0");
                                countryMap.put("Republic of Korea", "0");
                                countryMap.put("Russian Federation", "0");
                                countryMap.put("Saudi Arabia", "0");
                                countryMap.put("South Africa", "0");
                                countryMap.put("Spain", "0");
                                countryMap.put("Thailand", "0");
                                countryMap.put("Turkey", "0");
                                countryMap.put("U.K. of Great Britain and Northern Ireland", "0");
                                countryMap.put("Ukraine", "0");
                                countryMap.put("United States of America", "0");
                                countryMap.put("Viet Nam", "0");

                                countryList_map.get(key);
                                LinkedList<String> countryArray = countryList_map.get(key);
//                        System.out.println("countryArray");
//                        System.out.println(countryArray);
//
//                        System.out.println(countryMap.get("Argentina"));
//                        System.out.println(countryMap.get("Australia"));
//                        System.out.println(countryMap.get("Brazil"));
//                        System.out.println(countryMap.get("Canada"));
//                        System.out.println(countryMap.get("China"));
//                        System.out.println(countryMap.get("Egypt"));
//                        System.out.println(countryMap.get("European Union"));
//                        System.out.println(countryMap.get("France"));
//                        System.out.println(countryMap.get("Germany"));
//                        System.out.println(countryMap.get("India"));
//                        System.out.println(countryMap.get("Indonesia"));
//                        System.out.println(countryMap.get("Italy"));
//                        System.out.println(countryMap.get("Japan"));
//                        System.out.println(countryMap.get("Kazakhstan"));
//                        System.out.println(countryMap.get("Mexico"));
//                        System.out.println(countryMap.get("Nigeria"));
//                        System.out.println(countryMap.get("Philippines"));
//                        System.out.println(countryMap.get("Republic of Korea"));
//                        System.out.println(countryMap.get("Russian Federation"));
//                        System.out.println(countryMap.get("Saudi Arabia"));
//                        System.out.println(countryMap.get("South Africa"));
//                        System.out.println(countryMap.get("Spain"));
//                        System.out.println(countryMap.get("Thailand"));
//                        System.out.println(countryMap.get("Turkey"));
//                        System.out.println(countryMap.get("U.K. of Great Britain and Northern Ireland"));
//                        System.out.println(countryMap.get("Ukraine"));
//                        System.out.println(countryMap.get("United States of America"));
//                        System.out.println(countryMap.get("Viet Nam"));

                                for(int iCountry =0; iCountry<countryArray.size(); iCountry++){
                                    String countryName = countryArray.get(iCountry);
                                    countryMap.put(countryName, "1");
                                }

                                //System.out.println("INSIDE!!! ");
                                excel_map.put(time, countryMap);
                                previus_month = month;
//                                System.out.println("Key= "+ key);
//                                System.out.println("PolicyType= "+excel_map.get(time).get("PolicyType"));
//                                System.out.println("SUM= "+excel_map.get(time).get("SUM"));
//                        System.out.println("ARGENTINA= "+excel_map.get(time).get("Argentina"));
//                        System.out.println("AUSTRALIA= "+excel_map.get(time).get("Australia"));
//                        System.out.println("BRAZIL= "+excel_map.get(time).get("Brazil"));
//                        System.out.println("CANADA= "+excel_map.get(time).get("Canada"));
//                        System.out.println("CHINA= "+excel_map.get(time).get("China"));
//                        System.out.println("EGYPT= "+excel_map.get(time).get("Egypt"));
                            }
                        }
                        //                    System.out.println("key= "+key+ " "+"countPlus= "+countryCount_map.get(key));
//                    System.out.println("countryList_map.get(key)");
//                    System.out.println(countryList_map.get(key));
                    } catch (ParseException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
                pt_map.put(policy_meaure_code_array[i], countryCount_map);
                excel_map_array[i]=excel_map;
            }
            excel_map_array_pt_pm[z]= excel_map_array;
        }

//        Set<String> mapString = pt_map.keySet();
//        for(String key :mapString){
//            LinkedHashMap<String, String> list = pt_map.get(key);
//            Set<String> listString = list.keySet();
//            for(String elemlist: listString){
//                System.out.println("biofuelPolicyMeasures_timeSeries policy measure code key= "+key+" time elemlist= "+elemlist+ " count value = "+list.get(elemlist));
//            }
//        }
//        return pt_map;
        return excel_map_array_pt_pm;
    }

    //Original
//    public Map<String, LinkedHashMap<String, String>> exportRestrictionsPolicyMeasures_timeSeries(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
//        // System.out.println("biofuelPolicies_timeSeries start ");
//        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
//        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
//        String single_policy_type = policy_type_code_array[0];
//        String policy_meaure_code_array[]= pd_obj.getPolicy_measure_code();
//        for(int i=0; i<policy_meaure_code_array.length; i++)
//        {
//            String single_policy_measure = policy_meaure_code_array[i];
//            StringBuilder timeSeries_query = getStringQueryExportRestrictionsPolicyMeasures_TimeSeries(pd_obj, single_policy_type, single_policy_measure);
//
//            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
//            it.query(dsBean, timeSeries_query.toString());
//            //it.closeConnection();
//            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
//            while(it.hasNext()) {
//                //[2006-01-01, India]
//                //System.out.println(it.next());
//                String time_country = it.next().toString();
//                String time = time_country.substring(1, time_country.indexOf(','));
//                // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time );
//                if(countryCount_map.containsKey(time))
//                {
//                    String country_cont= countryCount_map.get(time);
//                    Integer count = Integer.parseInt(country_cont);
//                    count++;
//                    countryCount_map.put(time, ""+count);
////                    System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time+" count "+countryCount_map.get(time) );
//                    //countryCount_map.get(time).
//                }
//                else
//                {
//                    countryCount_map.put(time, "1");
//
//                }
//                // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time+" count "+countryCount_map.get(time) );
//            }
//            pt_map.put(policy_meaure_code_array[i], countryCount_map);
//        }
//
//        return pt_map;
//    }

    public Map<String, LinkedHashMap<String, String>> exportRestrictionsPolicyMeasures_timeSeries(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        // System.out.println("biofuelPolicies_timeSeries start ");
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        StringBuilder generate_query = getGenerateSeries(pd_obj);
        String single_policy_type = policy_type_code_array[0];
        String policy_meaure_code_array[]= pd_obj.getPolicy_measure_code();
        for(int i=0; i<policy_meaure_code_array.length; i++)
        {
            final JDBCIterablePolicy it_generateSeries =  new JDBCIterablePolicy();
            it_generateSeries.query(dsBean, generate_query.toString());
            String single_policy_measure = policy_meaure_code_array[i];

            StringBuilder timeSeries_query = getStringQueryExportRestrictionsPolicyMeasures_TimeSeries(pd_obj, single_policy_type, single_policy_measure);

            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
            LinkedHashMap<String, LinkedList<String>> countryList_map = new LinkedHashMap<String, LinkedList<String>>();
            while(it_generateSeries.hasNext()) {
                String days = it_generateSeries.next().toString();
                days = days.substring(1,days.length()-1);
                countryCount_map.put(days, "0");
                countryList_map.put(days, new LinkedList<String>());
            }

            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
            LinkedList<String []> timeSeriesList = new LinkedList<String[]>();
            it.query(dsBean, timeSeries_query.toString());

            while(it.hasNext()) {
                //[2006-01-01, India]
                //[India, 2006-01-01, 2010-01-01]
                String time_country = it.next().toString();
                //Removing []
                time_country = time_country.substring(1, time_country.length() - 1);
                //String  to array
                String[] result = time_country.split(", ");

                String country = result[0];
                String start_date = result[1];
                String end_date = result[2];
                String timeseriesArray[] = {country,start_date,end_date};
                timeSeriesList.add(timeseriesArray);
            }

            Set<String> keySet = countryCount_map.keySet();
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            for(String key: keySet){
                Date date = null;
                try {
                    date = df.parse(key);
                    for(int iEl=0; iEl<timeSeriesList.size(); iEl++){
                        String array[]= timeSeriesList.get(iEl);
                        //Array = country,start_date,end_date
                        Date start_date_from_query = null;
                        Date end_date_from_query = null;

                        start_date_from_query = df.parse(array[1]);
                        end_date_from_query = df.parse(array[2]);

                        //Compare the date
                        if((date.compareTo(start_date_from_query)>=0) && (date.compareTo(end_date_from_query)<=0)){
                            if(!countryList_map.get(key).contains(array[0])){
                                countryList_map.get(key).add(array[0]);
                                int count = Integer.parseInt(countryCount_map.get(key));
                                int countPlus = count+1;
                                countryCount_map.put(key, ""+countPlus);
                            }
                        }
                    }
                } catch (ParseException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }

            pt_map.put(policy_meaure_code_array[i], countryCount_map);
        }

//        Set<String> mapString = pt_map.keySet();
//        for(String key :mapString){
//            LinkedHashMap<String, String> list = pt_map.get(key);
//            Set<String> listString = list.keySet();
//            for(String elemlist: listString){
//                System.out.println("exportRestrictionsPolicyMeasures_timeSeries policy measure code key= "+key+" time elemlist= "+elemlist+ " count value = "+list.get(elemlist));
//            }
//        }

        return pt_map;
    }

    public Map<String, LinkedHashMap<String, String>> exportSubsidiesPolicyMeasures_timeSeries(DatasourceBean dsBean, POLICYDataObject pd_obj, String cpl_id) throws Exception{

        String start_date = pd_obj.getStart_date();
        //1995-01-01
//        int year_index = start_date.lastIndexOf('/');
//        String year = start_date.substring(year_index+1);
        String year = start_date.substring(0,4);
        Integer start_year_integer = Integer.parseInt(year);
        String end_date = pd_obj.getEnd_date();
//        year_index = end_date.lastIndexOf('/');
//        year = end_date.substring(year_index+1);
        year = end_date.substring(0,4);
        Integer end_year_integer = Integer.parseInt(year);

        Map<String, LinkedHashMap<String, String>> pelem_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();

        String policy_element_array[]= pd_obj.getPolicy_element();
       // String policy_element_array[]= null;

        for(int i=0; i<policy_element_array.length; i++)
        {
            String single_policy_element = "'"+policy_element_array[i]+"'";
            StringBuilder timeSeries_query = getStringQueryExportSubsidiesPolicyMeasures_TimeSeries(pd_obj, cpl_id, single_policy_element);

            final JDBCIterablePolicy it =  new JDBCIterablePolicy();
            it.query(dsBean, timeSeries_query.toString());
            //it.closeConnection();
            LinkedHashMap<String, String> policyElementValue_map = new LinkedHashMap<String, String>();
            for(int j=start_year_integer; j<=end_year_integer; j++)
            {
                policyElementValue_map.put(""+j,"");
            }
            while(it.hasNext()) {
                //[1995, 2238378.1]
                String time_policyElem = it.next().toString();
                String time = time_policyElem.substring(1, time_policyElem.indexOf(','));
                String value = time_policyElem.substring(time_policyElem.indexOf(',')+1, time_policyElem.length()-1);
                // System.out.println("single_policy_type "+single_policy_type+" time_country= "+time_country + " time "+time );
                policyElementValue_map.put(time,value);
            }
            pelem_map.put(policy_element_array[i], policyElementValue_map);
        }
        return pelem_map;
    }

    //Original
//    public StringBuilder getStringQueryBiofuelPolicyTypes_TimeSeries(POLICYDataObject pd_obj, String single_policy_type) throws Exception{
//
//        //select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=6 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday limit 50;
//        //select a.byday, a.country_name from (select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=9 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday)a GROUP BY a.byday, a.country_name order by a.byday limit 20;
//        String start_date = pd_obj.getStart_date();
//        String end_date = pd_obj.getEnd_date();
//        StringBuilder sb = new StringBuilder();
//        sb.append("select a.byday, a.country_name from (");
//        //sb.append("select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name");
////        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name");
////        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM') AS byday, tot.country_name");
////        sb.append("select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN '2024-12-31' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//   /*     start_date = "2003-01-01";
//        end_date = "2024-12-31";*/
//        start_date = "2010-01-01";
//        end_date = "2012-02-01";
//
//
////        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN '"+end_date+"' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, '"+end_date+"'::timestamp, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        sb.append(" from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable");
//        sb.append(" JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable ");
//        //((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policytable.start_date < '05/05/2010'))))
//        sb.append("WHERE ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//        sb.append(")policy");
//
//        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
////        sb.append(" ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
//        sb.append(") tot ");
//        sb.append(" where tot.policytype_code= "+single_policy_type+" GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday ASC");
//        sb.append(")a GROUP BY a.byday, a.country_name order by a.byday ASC");
//        System.out.println("getStringQueryBiofuelPolicyTypes_TimeSeries sb "+sb.toString());
//
//        return sb;
//    }

    //Backup
    //policyTable -> policyTableViewTest
    public StringBuilder getStringQueryBiofuelPolicyTypes_TimeSeries(POLICYDataObject pd_obj, String single_policy_type) throws Exception{
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();

//        start_date = "2003-01-01";
        start_date = "2011-01-01";
//        end_date = "2024-12-31";
//        start_date = "2010-01-01";
        end_date = "2014-12-31";
        //String end_date_to_show = "2016-01-01";

        sb.append("select a.country_name, a.start_date, CASE WHEN a.end_date IS NULL THEN '"+end_date+"' ELSE a.end_date END AS end_date from (");
//        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN '"+end_date+"' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
        sb.append("select tot.country_name, tot.start_date, tot.end_date");
        sb.append(" from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable");
        sb.append(" JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date from policyTableViewTest ");
        sb.append("WHERE ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR ((policyTableViewTest.start_date <= '"+start_date+"') AND (policyTableViewTest.end_date>='"+end_date+"')) OR (policyTableViewTest.end_date IS NULL AND ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.start_date < '"+start_date+"'))))");
        sb.append(")policy");

        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
        sb.append(") tot ");
        sb.append(" where tot.policytype_code= "+single_policy_type+" GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, tot.end_date, tot.start_date");
        sb.append(")a GROUP BY a.country_name, a.start_date, a.end_date order by a.start_date ASC");
        System.out.println("getStringQueryBiofuelPolicyTypes_TimeSeries sb "+sb.toString());

        return sb;
    }

    //policyTable -> policyTableViewTest
    public StringBuilder getStringQueryBiofuelPolicyTypes_TimeSeriesWithoutWTO(POLICYDataObject pd_obj, String single_policy_type, String policy_measure) throws Exception{
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();
        String importMeasurePolicyTypeCode = "2";
        start_date = "2003-01-01";
        //start_date = "2011-01-01";
        end_date = "2024-12-31";
        //end_date = "2014-12-31";

        sb.append("select a.country_name, a.start_date, CASE WHEN a.end_date IS NULL THEN '"+end_date+"' ELSE a.end_date END AS end_date from (");
//        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN '"+end_date+"' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
        sb.append("select tot.country_name, tot.start_date, tot.end_date");
        sb.append(" from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable");
        sb.append(" JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date from policyTableViewTest ");
        sb.append("WHERE ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR ((policyTableViewTest.start_date <= '"+start_date+"') AND (policyTableViewTest.end_date>='"+end_date+"')) OR (policyTableViewTest.end_date IS NULL AND ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.start_date < '"+start_date+"'))))");
        sb.append(")policy");

        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
        sb.append(") tot ");
        //System.out.println("getStringQueryBiofuelPolicyTypes_TimeSeriesWithoutWTO single_policy_type= " + single_policy_type + " importMeasurePolicyTypeCode= "+importMeasurePolicyTypeCode);
        //if(single_policy_type!= importMeasurePolicyTypeCode){
        if(!single_policy_type.equalsIgnoreCase(importMeasurePolicyTypeCode)){
            sb.append(" where tot.policytype_code= "+single_policy_type+" GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, tot.end_date, tot.start_date");
        }
        else{
            //System.out.println("UGUALI");
            sb.append(" where tot.policytype_code= "+single_policy_type+" and tot.policymeasure_code NOT IN ("+policy_measure+") GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, tot.end_date, tot.start_date");
        }
        sb.append(")a GROUP BY a.country_name, a.start_date, a.end_date order by a.start_date ASC");
        System.out.println("getStringQueryBiofuelPolicyTypes_TimeSeries sb "+sb.toString());

        return sb;
    }

    public StringBuilder getGenerateSeries(POLICYDataObject pd_obj) throws Exception{
        StringBuilder sb = new StringBuilder();
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        start_date = "2003-01-01";
        end_date = "2024-12-31";
//        start_date = "2010-01-01";
//        end_date = "2010-02-28";
//        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, '"+end_date+"'::timestamp, '1 DAY'), 'YYYY-MM-DD')");
        sb.append("select generate_series('"+start_date+"'::timestamp, '"+end_date+"'::timestamp, '1 DAY') + interval '2 hour'");
        System.out.println("SB!!!!");
        System.out.println(sb.toString());
        return sb;
    }

    //Original
//    public StringBuilder getStringQueryBiofuelPolicyMeasures_TimeSeries(POLICYDataObject pd_obj, String single_policy_type, String single_policy_measure) throws Exception{
//
//        //select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=6 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday limit 50;
//        //select a.byday, a.country_name from (select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=9 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday)a GROUP BY a.byday, a.country_name order by a.byday limit 20;
//        String start_date = pd_obj.getStart_date();
//        String end_date = pd_obj.getEnd_date();
//        StringBuilder sb = new StringBuilder();
//        start_date = "2003-01-01";
//        //end_date = "2020-12-31";
//        end_date = "2024-12-31";
//        sb.append("select a.byday, a.country_name from (");
////        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        //sb.append("select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN '2024-12-31' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN '"+end_date+"' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        sb.append(" from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable");
//        sb.append(" JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable ");
//        sb.append("WHERE ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//        sb.append(")policy");
//
//        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
////        sb.append(" ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
//        sb.append(") tot ");
//        sb.append(" where tot.policytype_code= "+single_policy_type+" and tot.policymeasure_code= "+single_policy_measure+" GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.policymeasure_code, tot.policymeasure_name, tot.country_code, tot.country_name, byday ORDER BY byday ASC");
//        sb.append(")a GROUP BY a.byday, a.country_name order by a.byday ASC");
//        System.out.println("getStringQueryBiofuelPolicyMeasures_TimeSeries sb "+sb.toString());
//
//        return sb;
//    }

    //policytable -> policyTableViewTest
    public StringBuilder getStringQueryBiofuelPolicyMeasures_TimeSeries(POLICYDataObject pd_obj, String single_policy_type, String single_policy_measure) throws Exception{

//        select a.country_name, a.start_date, CASE WHEN a.end_date IS NULL THEN '2024-12-31' ELSE a.end_date END AS end_date from
//        (select tot.country_name, tot.start_date, tot.end_date from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name,
//        mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date,
//        policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable WHERE
//        ((policytable.start_date BETWEEN '2003-01-01' AND '2024-12-31') OR (policytable.end_date BETWEEN '2003-01-01' AND '2024-12-31') OR (policytable.end_date IS NULL AND
//        ((policytable.start_date BETWEEN '2003-01-01' AND '2024-12-31') OR (policytable.start_date < '2003-01-01')))))policy ON mastertable.cpl_id = policy.cpl_id AND
//        mastertable.commodityclass_code IN (7)) tot  where tot.policytype_code= 8 and tot.policymeasure_code= 36 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name,
//        tot.policymeasure_code, tot.policymeasure_name, tot.country_code, tot.country_name, tot.end_date, tot.start_date)a GROUP BY a.country_name, a.start_date, a.end_date order by a.start_date ASC

        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();
//        start_date = "2003-01-01";
//        end_date = "2024-12-31";

        start_date = "2011-01-01";
        end_date = "2014-12-31";
        sb.append("select a.country_name, a.start_date, CASE WHEN a.end_date IS NULL THEN '"+end_date+"' ELSE a.end_date END AS end_date from ");
        sb.append("(select tot.country_name, tot.start_date, tot.end_date");
        sb.append(" from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable");
        sb.append(" JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date from policyTableViewTest ");
        sb.append("WHERE ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.end_date BETWEEN '"+start_date+"' AND '"+end_date+"')  OR ((policyTableViewTest.start_date <= '"+start_date+"') AND (policyTableViewTest.end_date>='"+end_date+"')) OR (policyTableViewTest.end_date IS NULL AND ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.start_date < '"+start_date+"'))))");
        sb.append(")policy");

        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
        sb.append(") tot ");
        sb.append(" where tot.policytype_code= "+single_policy_type+" and tot.policymeasure_code= "+single_policy_measure+" GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.policymeasure_code, tot.policymeasure_name, tot.country_code, tot.country_name,  tot.end_date, tot.start_date)a ");
        sb.append("GROUP BY a.country_name, a.start_date, a.end_date order by a.start_date ASC");
        System.out.println("getStringQueryBiofuelPolicyMeasures_TimeSeries sb "+sb.toString());

        return sb;
    }

    //(pd_obj, cpl_id, single_policy_element)
    public StringBuilder getStringQueryExportSubsidiesPolicyMeasures_TimeSeries(POLICYDataObject pd_obj, String cpl_id, String single_policy_element) throws Exception{

        StringBuilder sb = new StringBuilder();
        sb.append("select EXTRACT(year FROM start_date) AS year_label, SUM(CAST (value AS FLOAT)) from policytable where cpl_id IN("+cpl_id+") and (EXTRACT(year FROM start_date)>=1995 and EXTRACT(year FROM start_date)<=2011) and policy_element ="+single_policy_element+" GROUP BY year_label ORDER BY year_label ASC;");

        System.out.println("sb = "+sb.toString());
        return sb;
    }

    //Original
//    public StringBuilder getStringQueryExportRestrictionsPolicyMeasures_TimeSeries(POLICYDataObject pd_obj, String single_policy_type, String single_policy_measure) throws Exception{
//
//        //select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=6 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday limit 50;
//        //select a.byday, a.country_name from (select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=9 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday)a GROUP BY a.byday, a.country_name order by a.byday limit 20;
//        String start_date = pd_obj.getStart_date();
//        String end_date = pd_obj.getEnd_date();
//        StringBuilder sb = new StringBuilder();
//        sb.append("select a.byday, a.country_name from (");
//        start_date = "2006-01-01";
////        end_date = "2014-12-31";
//        end_date = "2024-12-31";
////        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        //sb.append("select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN '2024-12-31' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN '"+end_date+"' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        sb.append(" from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date, policy.value_text from mastertable");
//        sb.append(" JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date, policytable.value_text from policytable ");
//        sb.append("WHERE ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.end_date IS NULL AND ((policytable.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policytable.start_date < '"+start_date+"'))))");
//        sb.append(")policy");
//        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
////        sb.append(" ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
//        sb.append(") tot ");
//        sb.append(" where tot.policytype_code= "+single_policy_type+" and tot.policymeasure_code= "+single_policy_measure+" and (tot.value_text<>'elim' OR tot.value_text IS NULL) GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.policymeasure_code, tot.policymeasure_name, tot.country_code, tot.country_name, byday ORDER BY byday ASC");
//        sb.append(")a GROUP BY a.byday, a.country_name order by a.byday ASC");
//        System.out.println("getStringQueryExportRestrictionsPolicyMeasures_TimeSeries sb "+sb.toString());
//
//        return sb;
//    }

    //policytable -> policyTableViewTest
    public StringBuilder getStringQueryExportRestrictionsPolicyMeasures_TimeSeries(POLICYDataObject pd_obj, String single_policy_type, String single_policy_measure) throws Exception{

        //select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=6 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday limit 50;
        //select a.byday, a.country_name from (select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=9 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday)a GROUP BY a.byday, a.country_name order by a.byday limit 20;
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();

//        start_date = "2006-01-01";
        start_date = "2007-01-01";
//        end_date = "2014-12-31";
//        end_date = "2016-12-31";
        end_date = "2014-12-31";
        sb.append("select a.country_name, a.start_date, CASE WHEN a.end_date IS NULL THEN '"+end_date+"' ELSE a.end_date END AS end_date from (");
//        sb.append("select to_char(generate_series('"+start_date+"'::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
        //sb.append("select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN '2024-12-31' ELSE tot.end_date END, interval '1 mon'), 'YYYY-MM-DD') AS byday, tot.country_name");
        sb.append("select tot.country_name, tot.start_date, tot.end_date from ");
        sb.append("(select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date, policy.value_text from mastertable");
        sb.append(" JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date, policyTableViewTest.value_text from policyTableViewTest ");
        sb.append("WHERE ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR ((policyTableViewTest.start_date <= '"+start_date+"') AND (policyTableViewTest.end_date>='"+end_date+"')) OR (policyTableViewTest.end_date IS NULL AND ((policyTableViewTest.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policyTableViewTest.start_date < '"+start_date+"'))))");
        sb.append(")policy");
        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+")");
//        sb.append(" ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
        sb.append(") tot ");
        sb.append(" where tot.policytype_code= "+single_policy_type+" and tot.policymeasure_code= "+single_policy_measure+" and (tot.value_text<>'elim' OR tot.value_text IS NULL) GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.policymeasure_code, tot.policymeasure_name, tot.country_code, tot.country_name, tot.end_date, tot.start_date)a");
        sb.append(" GROUP BY a.country_name, a.start_date, a.end_date order by a.start_date ASC");
        System.out.println("getStringQueryExportRestrictionsPolicyMeasures_TimeSeries sb "+sb.toString());

        return sb;
    }

    //Old with Start and End Date Start
//    public StringBuilder getStringQueryBiofuelPolicies_TimeSeries(POLICYDataObject pd_obj, String single_policy_type) throws Exception{
//
//        //select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=6 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday limit 50;
//        //select a.byday, a.country_name from (select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=9 GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday)a GROUP BY a.byday, a.country_name order by a.byday limit 20;
//        String start_date = pd_obj.getStart_date();
//        String end_date = pd_obj.getEnd_date();
//        StringBuilder sb = new StringBuilder();
//        sb.append("select a.byday, a.country_name from (");
//        sb.append("select to_char(generate_series(tot.start_date::timestamp, CASE WHEN tot.end_date IS NULL THEN CURRENT_DATE ELSE tot.end_date END, '1 DAY'), 'YYYY-MM-DD') AS byday, tot.country_name");
//        sb.append(" from (select mastertable.cpl_id, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable");
//        sb.append(" JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy");
//        sb.append(" ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") AND ");
////        sb.append(" ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
//        sb.append(" ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.start_date < '"+start_date+"'))))) tot ");
//        sb.append(" where tot.policytype_code= "+single_policy_type+" GROUP BY tot.cpl_id, tot.policytype_code, tot.policytype_name, tot.country_code, tot.country_name, byday ORDER BY byday ASC");
//        sb.append(")a GROUP BY a.byday, a.country_name order by a.byday ASC");
//        System.out.println("getStringQueryBiofuelPolicies_TimeSeries sb "+sb.toString());
//
//        return sb;
//    }
    //Old with Start and End Date End

    public JDBCIterablePolicy getpolicyTypes_fromPolicyDomain(DatasourceBean dsBean, String commodityDomainCodes) throws Exception {
        //select DISTINCT(policytype_code), policytype_name from mastertable where commoditydomain_code = 2 order by policytype_code;
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT DISTINCT(policytype_code), policytype_name FROM mastertable where commoditydomain_code IN ("+commodityDomainCodes+") order by policytype_name ASC ");
         System.out.println("getpolicyTypes_fromPolicyDomain sb "+sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public Map<String, LinkedHashMap<String, String>> biofuelPolicies_barchart(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        String commodity_class_code_array[]= pd_obj.getCommodity_class_code().split(",");
        //System.out.println("pd_obj.getCommodity_class_code() "+pd_obj.getCommodity_class_code()+" commodity_class_code_array "+commodity_class_code_array);
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        //This is used ONLY to check if the data belong to WTO
        String mesureCodeArray[]= pd_obj.getPolicy_measure_code();
        String policy_measure ="";
        for(int i=0; i< mesureCodeArray.length; i++){
            if(policy_measure.length()>0){
                policy_measure+= ",";
            }
            policy_measure+= mesureCodeArray[i];
        }

        for(int i=0; i<policy_type_code_array.length; i++)
        {
            String single_policy_type = policy_type_code_array[i];
            //System.out.println("single_policy_type "+single_policy_type);
            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
            for(int j=0; j<commodity_class_code_array.length; j++)
            {
                String single_commodity_class = commodity_class_code_array[j];
//                StringBuilder barchart_query = getStringQueryBiofuelPolicyTypes_barchart(pd_obj, single_policy_type, single_commodity_class);
                StringBuilder barchart_query = getStringQueryBiofuelPolicyTypes_barchartWithoutWTO(pd_obj, single_policy_type, single_commodity_class, policy_measure);

                final JDBCIterablePolicy it =  new JDBCIterablePolicy();
                it.query(dsBean, barchart_query.toString());
                //Counter for the country
                String country_count = "0";
                //Return one value that is the number of country
                while(it.hasNext()) {
                    //[8] -> 8
                    String next = it.next().toString();
                    int lastsquare = next.lastIndexOf("]");
                    country_count = next.substring(1,lastsquare);
                }
                //It will have three values
               // System.out.println("single_commodity_class "+single_commodity_class+" country_count "+country_count);
                countryCount_map.put(single_commodity_class, country_count);
            }
            pt_map.put(single_policy_type, countryCount_map);
        }
        return pt_map;
    }

    public Map<String, LinkedHashMap<String, String>> biofuelMeasures_barchart(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        String commodity_class_code_array[]= pd_obj.getCommodity_class_code().split(",");
        //System.out.println("pd_obj.getCommodity_class_code() "+pd_obj.getCommodity_class_code()+" commodity_class_code_array "+commodity_class_code_array);
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        String policy_measures_code_array[]= pd_obj.getPolicy_measure_code();
        //The request is for all the Policy Measures of a specific Policy Type
        String single_policy_type = policy_type_code_array[0];
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        for(int i=0; i<policy_measures_code_array.length; i++)
        {
            //System.out.println("single_policy_type "+single_policy_type);
            String single_policy_measure = policy_measures_code_array[i];
            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
            for(int j=0; j<commodity_class_code_array.length; j++)
            {
                String single_commodity_class = commodity_class_code_array[j];
                StringBuilder barchart_query = getStringQueryBiofuelPolicyMeasures_barchart(pd_obj, single_policy_type, single_policy_measure, single_commodity_class);

                final JDBCIterablePolicy it =  new JDBCIterablePolicy();
                it.query(dsBean, barchart_query.toString());
                //Counter for the country
                String country_count = "0";
                //Return one value that is the number of country
                while(it.hasNext()) {
                    //[8] -> 8
                    String next = it.next().toString();
                    int lastsquare = next.lastIndexOf("]");
                    country_count = next.substring(1,lastsquare);
                    //System.out.println("country_count "+country_count);
                }
                //It will have three values
                //System.out.println("single_commodity_class "+single_commodity_class+" country_count "+country_count);
                countryCount_map.put(single_commodity_class, country_count);
            }
            pt_map.put(single_policy_measure, countryCount_map);
        }

        return pt_map;
    }

    public Map<String, LinkedHashMap<String, String>> importTariffs_barchart(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        String commodity_class_code_array[]= pd_obj.getCommodity_class_code().split(",");
        //System.out.println("pd_obj.getCommodity_class_code() "+pd_obj.getCommodity_class_code()+" commodity_class_code_array "+commodity_class_code_array);
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        String policy_measures_code_array[]= pd_obj.getPolicy_measure_code();
        String policy_element[]= pd_obj.getPolicy_element();
        String year_list= pd_obj.getYear_list();
        String year_list_array[]= year_list.split(",");
        String year_list_string = "";
        for(int iYear = 0; iYear<year_list_array.length; iYear++)
        {
            year_list_string += "'"+year_list_array[iYear]+"-01-01',";
        }

        if(year_list.length()>0)
        {
            year_list_string = year_list_string.substring(0,year_list_string.length()-1);
        }
        //'Final bound tariff' and 'MFN applied tariff'

        boolean min = pd_obj.getChartType();
        String chart_type = "fbt";
        StringBuilder barchart_query_fbt = getStringQueryImportTariffs_barchart(pd_obj, min, chart_type, policy_type_code_array[0], policy_measures_code_array[0], year_list_string);
        final JDBCIterablePolicy it_fbt =  new JDBCIterablePolicy();
        chart_type = "mfn";
        StringBuilder barchart_query_mfn = getStringQueryImportTariffs_barchart(pd_obj, min, chart_type, policy_type_code_array[0], policy_measures_code_array[0], year_list_string);
        final JDBCIterablePolicy it_mfn =  new JDBCIterablePolicy();

//        LinkedHashMap<String, String> commodity_policyElem_map = new LinkedHashMap<String, String>();
        Map<String, LinkedHashMap<String, String>> commodity_policyElem_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        for(int i=0; i<commodity_class_code_array.length; i++)
        {
            for(int j=0; j<policy_element.length; j++)
            {
                String key = ""+commodity_class_code_array[i]+"_"+policy_element[j].replaceAll("\\s+","");

                LinkedHashMap<String, String> yearPercentage_map = new LinkedHashMap<String, String>();
                for(int z=0; z<year_list_array.length; z++)
                {
                    yearPercentage_map.put(year_list_array[z], "");
                }
                commodity_policyElem_map.put(key, yearPercentage_map);
            }
        }

        it_fbt.query(dsBean, barchart_query_fbt.toString());
        while(it_fbt.hasNext()) {
            String next = it_fbt.next().toString();
            //[18, 1, Final bound tariff, 2010-01-01]
            String row = (next.substring(1, next.length()-1)).replaceAll("\\s+","");
            String row_array[] = row.split(",");
            String percentage = row_array[0];
            String commodity = row_array[1];
            String policy_element_i = row_array[2];
            String year = row_array[3];
            String observation = row_array[4];
            int year_index = year.indexOf("-01-01");
            year = year.substring(0, year_index);
            LinkedHashMap<String, String> yearPercentage_map_2 = commodity_policyElem_map.get(""+commodity+"_"+policy_element_i);
            yearPercentage_map_2.put(year, percentage + "%"+observation);
        }

        it_mfn.query(dsBean, barchart_query_mfn.toString());
        while(it_mfn.hasNext()) {
            String next = it_mfn.next().toString();
            //[80, 2, MFN applied tariff, 2012-01-01]
            String row = (next.substring(1, next.length()-1)).replaceAll("\\s+","");
            String row_array[] = row.split(",");
            String percentage = row_array[0];
            String commodity = row_array[1];
            String policy_element_i = row_array[2];
            String year = row_array[3];
            String observation = row_array[4];
            int year_index = year.indexOf("-01-01");
            year = year.substring(0, year_index);
            LinkedHashMap<String, String> yearPercentage_map_2 = commodity_policyElem_map.get(""+commodity+"_"+policy_element_i);
            yearPercentage_map_2.put(year, percentage + "%"+observation);
        }
        return commodity_policyElem_map;
    }

    public Map<String, LinkedHashMap<String, String>> exportRestrictionsMeasures_barchart(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        String commodity_class_code_array[]= pd_obj.getCommodity_class_code().split(",");
        //System.out.println("pd_obj.getCommodity_class_code() "+pd_obj.getCommodity_class_code()+" commodity_class_code_array "+commodity_class_code_array);
        String policy_type_code_array[]= pd_obj.getPolicy_type_code();
        String policy_measures_code_array[]= pd_obj.getPolicy_measure_code();
        //The request is for all the Policy Measures of a specific Policy Type
        String single_policy_type = policy_type_code_array[0];
        Map<String, LinkedHashMap<String, String>> pt_map = new LinkedHashMap<String, LinkedHashMap<String, String>>();
        for(int i=0; i<policy_measures_code_array.length; i++)
        {
            //System.out.println("single_policy_type "+single_policy_type);
            String single_policy_measure = policy_measures_code_array[i];
            LinkedHashMap<String, String> countryCount_map = new LinkedHashMap<String, String>();
            for(int j=0; j<commodity_class_code_array.length; j++)
            {
                String single_commodity_class = commodity_class_code_array[j];
                StringBuilder barchart_query = getStringQueryExportRestrictionsPolicyMeasures_barchart(pd_obj, single_policy_type, single_policy_measure, single_commodity_class);

                final JDBCIterablePolicy it =  new JDBCIterablePolicy();
                it.query(dsBean, barchart_query.toString());
                //Counter for the country
                String country_count = "0";
                //Return one value that is the number of country
                while(it.hasNext()) {
                    //[8] -> 8
                    String next = it.next().toString();
                    int lastsquare = next.lastIndexOf("]");
                    country_count = next.substring(1,lastsquare);
                    //System.out.println("country_count "+country_count);
                }
                //It will have three values
                //System.out.println("single_commodity_class "+single_commodity_class+" country_count "+country_count);
                countryCount_map.put(single_commodity_class, country_count);
            }
            pt_map.put(single_policy_measure, countryCount_map);
        }

        return pt_map;
    }

    //policytable-> policyTableViewTest
    public StringBuilder getStringQueryBiofuelPolicyTypes_barchart(POLICYDataObject pd_obj, String single_policy_type, String single_commodity_class) throws Exception{

        //select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=2 and tot.commodityclass_code=5;
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();
        sb.append("select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable ");
//        sb.append("JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
        sb.append("JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date from policyTableViewTest)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR ((policy.start_date <= '"+start_date+"') AND (policy.end_date>='"+end_date+"')) OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.start_date < '"+start_date+"'))))) tot ");
        sb.append("where tot.policytype_code="+single_policy_type+" and tot.commodityclass_code="+single_commodity_class+"");

        System.out.println("getStringQueryBiofuelPolicyTypes_barchart sb "+sb.toString());
        return sb;
    }

    //policytable-> policyTableViewTest
    public StringBuilder getStringQueryBiofuelPolicyTypes_barchartWithoutWTO(POLICYDataObject pd_obj, String single_policy_type, String single_commodity_class, String policy_measure) throws Exception{

        //select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot where tot.policytype_code=2 and tot.commodityclass_code=5;
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();
        String importMeasurePolicyTypeCode = "2";
        sb.append("select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable ");
//        sb.append("JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
        sb.append("JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date from policyTableViewTest)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR ((policy.start_date <= '"+start_date+"') AND (policy.end_date>='"+end_date+"')) OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.start_date < '"+start_date+"'))))) tot ");
        System.out.println("getStringQueryBiofuelPolicyTypes_barchartWithoutWTO single_policy_type= *" + single_policy_type + "* importMeasurePolicyTypeCode= *"+importMeasurePolicyTypeCode+"*");
        if(!single_policy_type.equalsIgnoreCase(importMeasurePolicyTypeCode))
        {
            sb.append("where tot.policytype_code="+single_policy_type+" and tot.commodityclass_code="+single_commodity_class+"");
        }
        else {
            System.out.println("UGUALI");
            sb.append("where tot.policytype_code=" + single_policy_type + " and tot.policymeasure_code NOT IN (" + policy_measure + ") and tot.commodityclass_code=" + single_commodity_class + "");
        }

        System.out.println("getStringQueryBiofuelPolicyTypes_barchartWithoutWTO sb "+sb.toString());
        return sb;
    }

   //policytable -> policyTableViewTest
    public StringBuilder getStringQueryBiofuelPolicyMeasures_barchart(POLICYDataObject pd_obj, String single_policy_type, String single_policy_measure, String single_commodity_class) throws Exception{

        //select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '1/1/2013' AND '1/1/2014') OR (policy.end_date BETWEEN '1/1/2013' AND '1/1/2014') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '1/1/2013' AND '1/1/2014') OR (policy.start_date < '1/1/2013'))))) tot where tot.policytype_code=2 and tot.policymeasure_code=8 and tot.commodityclass_code=6
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();
        sb.append("select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable ");
//        sb.append("JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
        sb.append("JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date from policyTableViewTest)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR ((policy.start_date <= '"+start_date+"') AND (policy.end_date>='"+end_date+"')) OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.start_date < '"+start_date+"'))))) tot ");
        sb.append("where tot.policytype_code="+single_policy_type+ " and tot.policymeasure_code="+single_policy_measure+" and tot.commodityclass_code="+single_commodity_class+"");

        System.out.println("getStringQueryBiofuelPolicyMeasures_barchart sb "+sb.toString());
        return sb;
    }

//    public StringBuilder getStringQueryImportTariffs_barchart(POLICYDataObject pd_obj, boolean min, String chart_type, String single_policy_type, String single_policy_measure, String year_list) throws Exception{
//
//        /*select MAX(tot.mfn_value), tot.commodityclass_code, tot.mfn_policy_element, tot.start_date FROM (SELECT mfn_table.policy_element as mfn_policy_element, fbt_table.policy_element as fbt_policy_element, mfn_table.value as mfn_value, fbt_table.value as fbt_value, mfn_table.start_date, mfn_table.commodityclass_code from ((select * from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code=2 and policymeasure_code=11 and commodityclass_code IN (1,2,3,4) and value_text IS NULL and (start_date>='2010-01-01' AND start_date<='2014-01-01') and policy_element='MFN applied tariff' and units='%' and value IS NOT NULL) mfn_table INNER JOIN (select * from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code=2 and policymeasure_code=11 and commodityclass_code IN (1,2,3,4) and value_text IS NULL and (start_date>='2010-01-01' AND start_date<='2014-01-01') and policy_element='Final bound tariff' and units='%' and value IS NOT NULL) fbt_table ON mfn_table.start_date=fbt_table.start_date and mfn_table.commodityclass_code=fbt_table.commodityclass_code)) tot group by tot.commodityclass_code, tot.mfn_policy_element, tot.start_date ORDER BY tot.commodityclass_code, tot.mfn_policy_element, tot.start_date;*/
//        StringBuilder sb = new StringBuilder();
////        if(min)
////        {
////            if(chart_type.equalsIgnoreCase("mfn"))
////            {
////                sb.append("select MIN(tot.mfn_value), ");
////            }
////            else{
////                sb.append("select MIN(tot.fbt_value), ");
////            }
////        }
////        else{
////            if(chart_type.equalsIgnoreCase("mfn"))
////            {
////                sb.append("select MAX(tot.mfn_value), ");
////            }
////            else{
////                sb.append("select MAX(tot.fbt_value), ");
////            }
////        }
//
//        if(chart_type.equalsIgnoreCase("mfn"))
//        {
//            sb.append("select AVG(CAST (tot.mfn_value AS FLOAT)), ");
//        }
//        else{
//            sb.append("select AVG(CAST (tot.fbt_value AS FLOAT)), ");
//        }
////        sb.append("select AVG(CAST (tot.mfn_value AS FLOAT)), ");
//
//        if(chart_type.equalsIgnoreCase("mfn"))
//        {
//            sb.append("tot.commodityclass_code, tot.mfn_policy_element, tot.start_date, COUNT(*) FROM ");
//        }
//        else{
//            sb.append("tot.commodityclass_code, tot.fbt_policy_element, tot.start_date, COUNT(*) FROM ");
//        }
//        sb.append("(SELECT mfn_table.policy_element as mfn_policy_element, fbt_table.policy_element as fbt_policy_element, mfn_table.value as mfn_value, fbt_table.value as fbt_value, mfn_table.start_date, mfn_table.commodityclass_code from ");
//        sb.append("((select * from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code="+single_policy_type+" and policymeasure_code="+single_policy_measure+" and commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") ");
//        sb.append("and value_text IS NULL and start_date IN ("+year_list+") and policy_element='"+pd_obj.getPolicy_element()[0]+"' and units='"+pd_obj.getUnit()+"' and value IS NOT NULL) mfn_table ");
//        sb.append("INNER JOIN (select * from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code="+single_policy_type+" and policymeasure_code="+single_policy_measure+" and commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") ");
//        sb.append("and value_text IS NULL and start_date IN ("+year_list+") and policy_element='"+pd_obj.getPolicy_element()[1]+"' and units='"+pd_obj.getUnit()+"' and value IS NOT NULL) ");
//        sb.append("fbt_table ON mfn_table.start_date=fbt_table.start_date and mfn_table.commodityclass_code=fbt_table.commodityclass_code)) tot ");
//
//        if(chart_type.equalsIgnoreCase("mfn"))
//        {
//            sb.append("group by tot.commodityclass_code, tot.mfn_policy_element, tot.start_date ORDER BY tot.commodityclass_code, tot.mfn_policy_element, tot.start_date");
//        }
//        else{
//            sb.append("group by tot.commodityclass_code, tot.fbt_policy_element, tot.start_date ORDER BY tot.commodityclass_code, tot.fbt_policy_element, tot.start_date");
//        }
//
//        System.out.println(sb.toString());
//        return sb;
//    }

    public StringBuilder getStringQueryImportTariffs_barchart(POLICYDataObject pd_obj, boolean min, String chart_type, String single_policy_type, String single_policy_measure, String year_list) throws Exception{

        /*select AVG(CAST (tot.fbt_value AS FLOAT)), tot.commodityclass_code, tot.fbt_policy_element, tot.start_date, COUNT(*) FROM (SELECT mfn_table.policy_element as mfn_policy_element, fbt_table.policy_element as fbt_policy_element, mfn_table.value as mfn_value, fbt_table.value as fbt_value, mfn_table.start_date, mfn_table.commodityclass_code from ((select value, commodityclass_code, policy_element, start_date, policytable.commodity_id from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code=2 and policymeasure_code=11 and commodityclass_code IN (1,3,2,4) and value_text IS NULL and start_date IN ('2010-01-01','2011-01-01','2012-01-01','2013-01-01') and policy_element='Final bound tariff' and units='%' and value IS NOT NULL) mfn_table INNER JOIN (select value, commodityclass_code, policy_element, start_date, policytable.commodity_id from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code=2 and policymeasure_code=11 and commodityclass_code IN (1,3,2,4) and value_text IS NULL and start_date IN ('2010-01-01','2011-01-01','2012-01-01','2013-01-01') and policy_element='MFN applied tariff' and units='%' and value IS NOT NULL) fbt_table ON mfn_table.start_date=fbt_table.start_date and mfn_table.commodityclass_code=fbt_table.commodityclass_code and mfn_table.commodity_id=fbt_table.commodity_id)) tot group by tot.commodityclass_code, tot.fbt_policy_element, tot.start_date ORDER BY tot.commodityclass_code, tot.fbt_policy_element, tot.start_date;*/
        StringBuilder sb = new StringBuilder();

        if(chart_type.equalsIgnoreCase("mfn"))
        {
            sb.append("select AVG(CAST (tot.mfn_value AS FLOAT)), ");
        }
        else{
            sb.append("select AVG(CAST (tot.fbt_value AS FLOAT)), ");
        }
//        sb.append("select AVG(CAST (tot.mfn_value AS FLOAT)), ");

        if(chart_type.equalsIgnoreCase("mfn"))
        {
            sb.append("tot.commodityclass_code, tot.mfn_policy_element, tot.start_date, COUNT(*) FROM ");
        }
        else{
            sb.append("tot.commodityclass_code, tot.fbt_policy_element, tot.start_date, COUNT(*) FROM ");
        }
        sb.append("(SELECT mfn_table.policy_element as mfn_policy_element, fbt_table.policy_element as fbt_policy_element, mfn_table.value as mfn_value, fbt_table.value as fbt_value, mfn_table.start_date, mfn_table.commodityclass_code from ");
        sb.append("((select value, commodityclass_code, policy_element, start_date, policytable.commodity_id from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code="+single_policy_type+" and policymeasure_code="+single_policy_measure+" and commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") ");
        sb.append("and value_text IS NULL and start_date IN ("+year_list+") and policy_element='"+pd_obj.getPolicy_element()[0]+"' and units='"+pd_obj.getUnit()+"' and value IS NOT NULL) mfn_table ");
        sb.append("INNER JOIN (select value, commodityclass_code, policy_element, start_date, policytable.commodity_id from policytable, mastertable where policytable.cpl_id = mastertable.cpl_id and policytype_code="+single_policy_type+" and policymeasure_code="+single_policy_measure+" and commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") ");
        sb.append("and value_text IS NULL and start_date IN ("+year_list+") and policy_element='"+pd_obj.getPolicy_element()[1]+"' and units='"+pd_obj.getUnit()+"' and value IS NOT NULL) ");
        sb.append("fbt_table ON mfn_table.start_date=fbt_table.start_date and mfn_table.commodityclass_code=fbt_table.commodityclass_code and mfn_table.commodity_id=fbt_table.commodity_id)) tot ");

        if(chart_type.equalsIgnoreCase("mfn"))
        {
            sb.append("group by tot.commodityclass_code, tot.mfn_policy_element, tot.start_date ORDER BY tot.commodityclass_code, tot.mfn_policy_element, tot.start_date");
        }
        else{
            sb.append("group by tot.commodityclass_code, tot.fbt_policy_element, tot.start_date ORDER BY tot.commodityclass_code, tot.fbt_policy_element, tot.start_date");
        }

        System.out.println("getStringQueryImportTariffs_barchart "+sb.toString());
        return sb;
    }

    //policyTable -> policyTableViewTest
    public StringBuilder getStringQueryExportRestrictionsPolicyMeasures_barchart(POLICYDataObject pd_obj, String single_policy_type, String single_policy_measure, String single_commodity_class) throws Exception{

        //select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date from mastertable JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '1/1/2013' AND '1/1/2014') OR (policy.end_date BETWEEN '1/1/2013' AND '1/1/2014') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '1/1/2013' AND '1/1/2014') OR (policy.start_date < '1/1/2013'))))) tot where tot.policytype_code=2 and tot.policymeasure_code=8 and tot.commodityclass_code=6
        String start_date = pd_obj.getStart_date();
        String end_date = pd_obj.getEnd_date();
        StringBuilder sb = new StringBuilder();
        sb.append("select count(DISTINCT(tot.country_code)) from (select mastertable.cpl_id, mastertable.commodityclass_code, mastertable.commodityclass_name, mastertable.policytype_code, mastertable.policytype_name, mastertable.policymeasure_code, mastertable.policymeasure_name, mastertable.country_code, mastertable.country_name, policy.start_date, policy.end_date, policy.value_text from mastertable ");
//        sb.append("JOIN (select policytable.cpl_id, policytable.start_date, policytable.end_date from policytable)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN (5,6,7) AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '05/05/2010' AND '05/04/2011') OR (policy.start_date < '05/05/2010'))))) tot ");
        sb.append("JOIN (select policyTableViewTest.cpl_id, policyTableViewTest.start_date, policyTableViewTest.end_date, policyTableViewTest.value_text from policyTableViewTest)policy ON mastertable.cpl_id = policy.cpl_id AND mastertable.commodityclass_code IN ("+pd_obj.getCommodity_class_code()+") AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.end_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR ((policy.start_date <= '"+start_date+"') AND (policy.end_date>='"+end_date+"')) OR (policy.end_date IS NULL AND ((policy.start_date BETWEEN '"+start_date+"' AND '"+end_date+"') OR (policy.start_date < '"+start_date+"'))))) tot ");
        sb.append("where tot.policytype_code="+single_policy_type+ " and tot.policymeasure_code="+single_policy_measure+" and tot.commodityclass_code="+single_commodity_class+" AND (tot.value_text<>'elim' OR tot.value_text IS NULL)");

        System.out.println("getStringQueryExportRestrictionsPolicyMeasures_barchart sb "+sb.toString());
        return sb;
    }

    //For Policy Data Entry
    public JDBCIterablePolicy getUnits(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT DISTINCT(units) from policytable ORDER BY units ASC");
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getPolicyElement(DatasourceBean dsBean, String policy_measure_code) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("select DISTINCT(policytable.policy_element) from mastertable, policytable where mastertable.cpl_id = policytable.cpl_id and  mastertable.policymeasure_code = "+policy_measure_code+" ORDER BY policytable.policy_element ASC");
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getSource(DatasourceBean dsBean, String countryCode) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("select DISTINCT(source) from policytable, mastertable WHERE country_code= "+countryCode+" and mastertable.cpl_id= policytable.cpl_id ORDER BY source ASC");
        //System.out.println("getSource end " + sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //select DISTINCT(source) from policytable where source NOT LIKE '%WTO%'
    public JDBCIterablePolicy getSourceWithoutWTOSource(DatasourceBean dsBean, String countryCode) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("select DISTINCT(source) from policytable, mastertable WHERE country_code= "+countryCode+" and mastertable.cpl_id= policytable.cpl_id and source NOT LIKE '%WTO%'ORDER BY source ASC");
        System.out.println("getSourceWithoutWTOSource end " + sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getSecondGenerationSpecific(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("select DISTINCT(second_generation_specific) FROM policytable ORDER BY second_generation_specific ASC");
        //System.out.println("getSource end "+ sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getImposedEndDate(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("select DISTINCT(imposed_end_date) FROM policytable ORDER BY imposed_end_date ASC");
        //System.out.println("getSource end "+ sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getLocalCondition(DatasourceBean dsBean) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("select DISTINCT(location_condition) from policytable ORDER BY location_condition ASC");
        //System.out.println("getLocalCondition "+ sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getCondition(DatasourceBean dsBean, String policyDomainCode, String policyTypeCode, String commodityDomainCode) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("select DISTINCT(condition_code)||Policycondition, condition from mastertable ORDER BY condition ASC");
        sb.append("select DISTINCT(condition_code), condition from mastertable where policydomain_code IN ("+policyDomainCode+") and policytype_code="+policyTypeCode+" and commoditydomain_code IN ("+commodityDomainCode+") ORDER BY condition ASC");
        System.out.println("getCondition "+ sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    public JDBCIterablePolicy getIndividualPolicy(DatasourceBean dsBean, String policyDomainCode, String policyTypeCode, String commodityDomainCode) throws Exception {
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
//        sb.append("select DISTINCT(condition_code)||Policycondition, condition from mastertable ORDER BY condition ASC");
        sb.append("select DISTINCT(individualpolicy_code), individualpolicy_name from mastertable where policydomain_code IN ("+policyDomainCode+") and policytype_code="+policyTypeCode+" and commoditydomain_code IN ("+commodityDomainCode+") ORDER BY individualpolicy_name ASC");
        //System.out.println("getIndividualPolicy "+ sb.toString());
        it.query(dsBean, sb.toString());
        return it;
    }

    //This function returns all the policy fields using the cpl_id
    public JDBCIterablePolicy createNewSingleCommodity(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        //Check if the commodity is already in the database
        JDBCIterablePolicy itSelect = new JDBCIterablePolicy();
        String selectCommodity = "SELECT * FROM commlistwithid WHERE ";
        selectCommodity+= "country_name = '"+pd_obj.getCountry_name()+"' AND hs_code = '"+ pd_obj.getHs_code()+"' AND hs_suffix = '"+ pd_obj.getHs_suffix()+"' AND hs_version = '"+ pd_obj.getHs_version()+"' AND description = '"+ pd_obj.getDescription()+"' AND short_description = '"+ pd_obj.getShort_description()+"' AND commodityclass_name = '"+ pd_obj.getCommodity_class_name()+"' AND commodityclass_code = '"+ pd_obj.getCommodity_class_code()+"' AND shared_group_code = '"+pd_obj.getShared_group_code()+"' AND country_code = '"+ pd_obj.getCountry_code()+"'";
        System.out.println("selectCommodity= "+selectCommodity);
        itSelect.query(dsBean, selectCommodity);
        JDBCIterablePolicy itInsert= new JDBCIterablePolicy();
        if(!itSelect.hasNext()){
            //If the commodity is not duplicated
            JDBCIterablePolicy it = new JDBCIterablePolicy();
            StringBuilder sb = new StringBuilder();
            StringBuilder sbInsert = new StringBuilder();
            String commodityQuery = "SELECT MAX(commodity_id) from commlistwithid";
            StringBuilder sbCommodityQuery = new StringBuilder();
            sb.append(commodityQuery);
            it.query(dsBean, sb.toString());
            if(it.hasNext()){
                String max_commodity_id = it.next().toString();
                max_commodity_id = max_commodity_id.substring(1, max_commodity_id.length() - 1);
                //System.out.println("max_commodity_id = "+max_commodity_id);
                Integer maxCommodity = Integer.parseInt(max_commodity_id);
                int newCommodityId = maxCommodity+1;

//                System.out.println(pd_obj.getCountry_code());
//                System.out.println(pd_obj.getCountry_name());
//                System.out.println(pd_obj.getHs_code());
////        System.out.println(pd_obj.getHs_suffix());
//                System.out.println(pd_obj.getHs_version());
//                System.out.println(pd_obj.getDescription());
//                System.out.println(pd_obj.getShort_description());
//                System.out.println(pd_obj.getCommodity_class_code());
//                System.out.println(pd_obj.getCommodity_class_name());
//                System.out.println(pd_obj.getShared_group_code());

                String insertQuery = "INSERT INTO commlistwithid (commodity_id,country_name,hs_code,hs_suffix,hs_version,description,short_description,commodityclass_name,commodityclass_code,shared_group_code,country_code)";
                insertQuery+= " VALUES ("+newCommodityId+",'"+pd_obj.getCountry_name()+"','"+ pd_obj.getHs_code()+"','"+ pd_obj.getHs_suffix()+"','"+ pd_obj.getHs_version()+"','"+ pd_obj.getDescription()+"','"+ pd_obj.getShort_description()+"','"+ pd_obj.getCommodity_class_name()+"','"+ pd_obj.getCommodity_class_code()+"','"+pd_obj.getShared_group_code()+"','"+ pd_obj.getCountry_code()+"')";
                System.out.println("createNewSingleCommodity = "+insertQuery);
                itInsert.queryInsert(dsBean, insertQuery);
            }
        }
        return itInsert;
    }

    //This function returns all the policy fields using the cpl_id
    public Boolean createNewSharedGroup(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        //Check if the Shared Group is already in the table
        //Check the code
        StringBuilder sbSharedGroupCode = new StringBuilder();
        String sharedGroupCodeQuery = "SELECT commodity_id, country_name, hs_code, hs_suffix, hs_version, description, short_description, commodityclass_name, commodityclass_code, shared_group_code, country_code from commlistwithid where shared_group_code like '%"+pd_obj.getShared_group_code()+"%'";
        sbSharedGroupCode.append(sharedGroupCodeQuery);
        it.query(dsBean, sbSharedGroupCode.toString());

//        System.out.println("SgCode start ");
//        System.out.println(sharedGroupCodeQuery);
        boolean sgFound = false;
        LinkedList<String> sgList_commodityId = new LinkedList<String>();
        String incrementalSgCode = "001";
        Integer codeCountInt = 0;
        while(it.hasNext()){
            sgFound = true;
            String result = it.next().toString();
            result = result.substring(1, result.length()-1);
            String sgCodeRes[] = result.split(",");
            String commodityId = sgCodeRes[0];
            sgList_commodityId.push(commodityId);
            //pd_obj.getShared_group_code() is composed by 4 digits (3 for Country Iso Code and a Letter)

            String codeCount = sgCodeRes[9].trim().substring(4);
            System.out.println("*"+codeCount+"*");
            codeCountInt = Integer.parseInt(codeCount);
            Integer newCodeCount = codeCountInt+1;
            String newCodeCountString = ""+newCodeCount;
            if(newCodeCountString.length()==1)
            {
                newCodeCountString = "00"+ newCodeCountString;
            }
            else if(newCodeCountString.length()==2)
            {
                newCodeCountString = "0"+ newCodeCountString;
            }
            incrementalSgCode = newCodeCountString;
        }
        //Adding the last part to the SG code
        pd_obj.setShared_group_code(pd_obj.getShared_group_code() + incrementalSgCode);

            if(sgFound){
            //This is the list of the commodity associated with that Shared Group
            LinkedHashMap<String, LinkedHashMap<String, String>> commodityList_sg = pd_obj.getCommodity_list();
            //Checks if the list of commodities in the database is the same of the list selected by the user
            //In this case cannot be created a duplicate
            for(int iCount=0; iCount<sgList_commodityId.size(); iCount++){
                JDBCIterablePolicy itCommInSG = new JDBCIterablePolicy();
                StringBuilder sbCommInSG = new StringBuilder();
                //Ordered By id_single that is the Commodity Id of the commodity that belong to the Shared Group
                String commodityList_forEachSG = "SELECT id_single from sharedgroups where commodity_id = "+Integer.parseInt(sgList_commodityId.get(iCount)) + " ORDER BY id_single ASC";
                System.out.println(commodityList_forEachSG);
                sbCommInSG.append(commodityList_forEachSG);
                itCommInSG.query(dsBean, sbCommInSG.toString());

                LinkedList<String> commodityIdListFromDb = new LinkedList<String>();
//                {0={hsCode=10059010, hsVersion=HS2007, shortDescription=Maize (corn) (-) En grano, originalHsCode=, originalHsVersion=, originalHsSuffix=, commodityId=96},
//                        1={hsCode=10059090, hsVersion=HS2007, shortDescription=Maize (corn) (-) Los demás, originalHsCode=, originalHsVersion=, originalHsSuffix=, commodityId=97},
//                        2={hsCode=11022000, hsVersion=HS2007, shortDescription=(-) Maize (corn) flour, originalHsCode=, originalHsVersion=, originalHsSuffix=, commodityId=110}}
                //Loop on the commodityList_sg to see if the ids are the same that have been found in the selection
                while(itCommInSG.hasNext()){
                    String commodityBelongSg = itCommInSG.next().toString();
                    commodityIdListFromDb.push(commodityBelongSg);
                }
                int iListFromDb =0;
                int iClientCommodityList = 0;
                int iDbCommodityList = 0;
                if((commodityIdListFromDb!=null)&&(commodityList_sg!=null)){
                    if(commodityIdListFromDb.size()==commodityList_sg.size()){
                        ArrayList<Boolean> found = new ArrayList<Boolean>();
                        for(iClientCommodityList = 0; iClientCommodityList<commodityList_sg.size(); iClientCommodityList++){
                            found.add(iClientCommodityList,false);
                        }
                        for(iClientCommodityList = 0; iClientCommodityList<commodityList_sg.size(); iClientCommodityList++){
                            for(iDbCommodityList = 0; iDbCommodityList<commodityIdListFromDb.size(); iDbCommodityList++){
                                if(commodityList_sg.get(""+iClientCommodityList).get("commodityId").equals(commodityIdListFromDb.get(iDbCommodityList))){
                                    //Equal
                                    found.add(iClientCommodityList,true);
                                    break;
                                }
                            }
                        }
                        for(iClientCommodityList = 0; iClientCommodityList<commodityList_sg.size(); iClientCommodityList++){
                            if(found.get(iClientCommodityList)==false){
                                break;
                            }
                        }
                        if(iClientCommodityList==commodityList_sg.size()){
                            //All Are True ... the two Shared Group are equal
                            //Not insert... Duplicated
                             System.out.println("Not insert... Duplicated ");
                            return false;
                        }
                        else {
                            //Insert
                            System.out.println("Found but not duplicated");
                            insertNewSharedGroup(dsBean, pd_obj);
                            return true;
                        }
                    }
                    else{
                        //Insert
                        System.out.println("Found but not duplicated 2");
                        insertNewSharedGroup(dsBean, pd_obj);
                        return true;
                    }
                }
            }
        }
        else{
            System.out.println("sgFound NOT FOUND!!! ");
            //Insert
            insertNewSharedGroup(dsBean,pd_obj);
            return true;
        }
        return true;
    }

    //This function returns all the policy fields using the cpl_id
    public void insertNewSharedGroup(DatasourceBean dsBean, POLICYDataObject pd_obj) throws Exception{
        createNewSingleCommodity(dsBean, pd_obj);

        //Select to read the commodity id of the Shared Group
        String sgCommodity = "SELECT commodity_id from commlistwithid where shared_group_code='"+pd_obj.getShared_group_code()+"'";
        JDBCIterablePolicy itSelect = new JDBCIterablePolicy();
        itSelect.query(dsBean, sgCommodity);
        LinkedHashMap<String, LinkedHashMap<String, String>> commodityList_sg = pd_obj.getCommodity_list();
//        String test1 = "2406";
        if(itSelect.hasNext()){
            String sg_commodity_id = itSelect.next().toString();
            sg_commodity_id = sg_commodity_id.substring(1, sg_commodity_id.length() - 1);
//            sg_commodity_id = test1;
            StringBuilder sbInsert = new StringBuilder();
            JDBCIterablePolicy itInsert = new JDBCIterablePolicy();
            //Insert all the records in the shared group table
//            System.out.println("commodityList_sg.size() "+commodityList_sg.size());
            Set<String> keys = commodityList_sg.keySet();
            Iterator<String> itCom =keys.iterator();
            while(itCom.hasNext()) {
                String keyIndex = itCom.next();
                itInsert = new JDBCIterablePolicy();
//                System.out.println("commodityList_sg Element " + commodityList_sg.get("" + keyIndex));
                LinkedHashMap<String,String> commodityInfoList =  commodityList_sg.get("" + keyIndex);
                Set<String> keys2 = commodityInfoList.keySet();
                Iterator<String> itCom2 = keys2.iterator();
                String id_single = "";
                String original_hs_version = "";
                String original_hs_code = "";
                String original_hs_suffix = "";
                while (itCom2.hasNext()) {
                    //System.out.println(itCom2.next());
                    String commodityInfo = itCom2.next();
                    if(commodityInfo.equals("commodityId")){
                        id_single = commodityInfoList.get(commodityInfo);
                    }
                    if(commodityInfo.equals("original_hs_version")){
                        original_hs_version = commodityInfoList.get(commodityInfo);
                    }
                    if(commodityInfo.equals("original_hs_code")){
                        original_hs_code = commodityInfoList.get(commodityInfo);
                    }
                    if(commodityInfo.equals("original_hs_suffix")){
                        original_hs_suffix = commodityInfoList.get(commodityInfo);
                    }
                }
                String insertQuery = "INSERT INTO sharedgroups (commodity_id, id_single, original_hs_version, original_hs_code, original_hs_suffix)";
                insertQuery += " VALUES (" + sg_commodity_id + "," + id_single + ", '" + original_hs_version + "', '" + original_hs_code + "', '" + original_hs_suffix + "')";
                System.out.println("insertNewSharedGroup=  "+insertQuery);
                itInsert.queryInsert(dsBean, insertQuery);
            }
        }
    }

    public JDBCIterablePolicy getMaxCodeNotGaul(DatasourceBean dsBean) throws Exception{
        System.out.println("getMaxCodeNotGaul");
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        String subnationalQuery = "SELECT MIN(subnational_code) from mastertableb";
        sb.append(subnationalQuery);
        it.query(dsBean, sb.toString());
        return  it;
    }

    public JDBCIterablePolicy getConditionMaxCode(DatasourceBean dsBean) throws Exception{
        System.out.println("getConditionMaxCode");
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        String conditionQuery = "SELECT MAX(condition_code) from mastertable";
        sb.append(conditionQuery);
        it.query(dsBean, sb.toString());
        return  it;
    }

    public JDBCIterablePolicy getCplIdMax(DatasourceBean dsBean) throws Exception{
        System.out.println("getCplIdMax");
        JDBCIterablePolicy it = new JDBCIterablePolicy();
        StringBuilder sb = new StringBuilder();
        String conditionQuery = "SELECT MAX(cpl_id) from mastertable";
        sb.append(conditionQuery);
        it.query(dsBean, sb.toString());
        return  it;
    }

    public Boolean saveInMasterTable(DatasourceBean dsBean, POLICYDataObject pd_obj, String key) throws Exception{
        System.out.println("In Save Master Table " +key);
        LinkedHashMap<String, LinkedHashMap<String, String>> fieldsToSave = pd_obj.getSave_fields();
        LinkedHashMap<String, String> master = fieldsToSave.get(key);
        JDBCIterablePolicy itInsert = new JDBCIterablePolicy();

        String column = "cpl_id, commodity_id, country_code, country_name, subnational_code, subnational_name, commoditydomain_code, commoditydomain_name, commodityclass_code, commodityclass_name, policydomain_code, policydomain_name, policytype_code, policytype_name, policymeasure_code, policymeasure_name, condition_code, condition, individualpolicy_code, individualpolicy_name";
//        String values[] = new String[column.length()];
        String values[] = new String[column.length()];
        values[0]="CommodityClassCode";
        values[1]="CommodityClassName";
        values[2]="CommodityDomainCode";
        values[3]="CommodityDomainName";
        values[4]="CommodityId";
        values[5]="CountryCode";
        values[6]="CountryName";
        values[7]="CplId";
        values[8]="IndividualPolicyCode";
        values[9]="IndividualPolicyName";
        values[10]="PolicyCondition";
        values[11]="PolicyConditionCode";
        values[12]="PolicyDomainCode";
        values[13]="PolicyDomainName";
        values[14]="PolicyMeasureCode";
        values[15]="PolicyMeasureName";
        values[16]="PolicyTypeCode";
        values[17]="PolicyTypeName";
        values[18]="PolicyDomainName";
        values[19]="SubnationalCode";
        values[20]="SubnationalName";
        String valuesInsertQuery = "";
        for(int iValues= 0; iValues<values.length; iValues++){
            System.out.println("values[iValues]="+values[iValues]);
            valuesInsertQuery += " '"+master.get(values[iValues])+"'";
            if(iValues<values.length-1){
                valuesInsertQuery+=',';
            }
        }

        String insertQuery = "INSERT INTO mastertable ("+column+")";
        insertQuery += " VALUES (" +valuesInsertQuery+")";
        System.out.println("saveInMasterTable=  "+insertQuery);
        //itInsert.queryInsert(dsBean, insertQuery);

        return false;
    }

    public Boolean saveInPolicyTable(DatasourceBean dsBean, POLICYDataObject pd_obj, String key) throws Exception{
        System.out.println("In Save Policy Table");
        LinkedHashMap<String, LinkedHashMap<String, String>> fieldsToSave = pd_obj.getSave_fields();
        LinkedHashMap<String, String> policy = fieldsToSave.get(key);
        LinkedHashMap<String, String> master = fieldsToSave.get("master");
        JDBCIterablePolicy itInsert = new JDBCIterablePolicy();

        String column = "metadata_id, policy_id, cpl_id, commodity_id, policy_element, start_date, end_date, units, value, value_text, value_type, exemptions, location_condition, notes, link, source, title_of_notice,";
        column+= " legal_basis_name, date_of_publication, imposed_end_date, second_generation_specific, benchmark_tax, benchmark_product, tax_rate_biofuel, tax_rate_benchmark, start_date_tax, benchmark_link,";
        column+= " original_dataset, type_of_change_code, type_of_change_name, measure_description, product_original_hs, product_original_name, link_pdf, benchmark_link_pdf, element_code";

        String values[] = new String[column.length()];
        values[0]="";
        values[18]="Policy_id";
        values[18]="CplId";
        values[18]="CommodityId";
        values[17]="PolicyElement";
        values[24]="StartDate";
        values[4]="EndDate";
        values[29]="Unit";
        values[30]="Value";
        values[31]="ValueText";
        values[32]="ValueType";
        values[32]="Exemptions";
        values[12]="LocalCondition";
        values[15]="Notes";
        values[10]="Link";
        values[23]="Source";
        values[27]="TitleOfNotice";
        values[9]="LegalBasisName";
        values[2]="DateOfPublication";
        values[8]="ImposedEndDate";
        values[0]="secondGenerationSpecific";
        values[26]="TaxRateBenchmark";
        values[0]="BenchmarkLink";
        values[1]="BenchmarkLinkPdf";
        values[3]="Description";
        values[5]="HsCode";
        values[6]="HsVersion";
        values[7]="HsSuffix";
        values[11]="LinkPdf";
        values[13]="MasterIndex";
        values[14]="MeasureDescription";
        values[16]="OriginalDataset";
        values[19]="ProductOriginalHs";
        values[20]="ProductOriginalName";
        values[21]="SharedGroupCode";
        values[22]="ShortDescription";
        values[25]="StartDateTax";
        values[26]="TaxRateBenchmark";
        values[28]="TypeOfChangeName";
        values[33]="typeOfChangeCode";
        values[34]="typeOfChangeName";
        values[35]="uid";

        String valuesInsertQuery = "";
        for(int iValues= 0; iValues<values.length; iValues++){
            valuesInsertQuery += " '"+policy.get(values[iValues])+"'";
            if(iValues<values.length-1){
                valuesInsertQuery+=',';
            }
        }

        String insertQuery = "INSERT INTO policytable ("+column+")";
        insertQuery += " VALUES (" +valuesInsertQuery+")";
        System.out.println("saveInPolicTable=  "+insertQuery);
        //itInsert.queryInsert(dsBean, insertQuery);

        return false;
    }
}