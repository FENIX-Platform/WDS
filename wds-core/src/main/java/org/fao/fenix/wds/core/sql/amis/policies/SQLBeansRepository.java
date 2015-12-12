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
package org.fao.fenix.wds.core.sql.amis.policies;

import org.fao.fenix.wds.core.bean.GroupByBean;
import org.fao.fenix.wds.core.bean.OrderByBean;
import org.fao.fenix.wds.core.bean.SQLBean;
import org.fao.fenix.wds.core.bean.WhereBean;
import org.fao.fenix.wds.core.constant.SQL;

import java.util.ArrayList;
import java.util.List;


public class SQLBeansRepository {

	public static SQLBean getEndDateYears(String table, List<WhereBean> whereBeans) {
		SQLBean sql = new SQLBean();
		sql.select(null, "EXTRACT(YEAR FROM to_date(\"End_Date\", 'DD-MM-YYYY'))", "code");
        sql.select(null, "EXTRACT(YEAR FROM to_date(\"End_Date\", 'DD-MM-YYYY'))", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"End_Date\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"End_Date\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("EXTRACT(YEAR FROM to_date(\"End_Date\", 'DD-MM-YYYY'))", "ASC"));
		sql.groupBy(new GroupByBean("EXTRACT(YEAR FROM to_date(\"End_Date\", 'DD-MM-YYYY'))"));
		return sql;
	}

    public static SQLBean getStartDateYears(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))", "code");
        sql.select(null, "EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"Start_Date\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"Start_Date\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))", "ASC"));
        sql.groupBy(new GroupByBean("EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))"));
        return sql;
    }


  /**  public static SQLBean getToDates(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))", "code");
        sql.select(null, "EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"Start_Date\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"Start_Date\"", "!=", "''", null);
        sql.orderBy(new OrderByBean("EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))", "DESC"));
        sql.groupBy(new GroupByBean("EXTRACT(YEAR FROM to_date(\"Start_Date\", 'DD-MM-YYYY'))"));
        return sql;
    }    **/


    public static SQLBean getCountries(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "\"Country_Code\"", "code");
        sql.select(null, "\"Country_Name\"", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"Country_Name\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"Country_Name\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("\"Country_Name\"", "ASC"));
        sql.groupBy(new GroupByBean("\"Country_Name\""));
        sql.groupBy(new GroupByBean("\"Country_Code\""));

        return sql;
    }

    public static SQLBean getPolicyDomains(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "\"PolicyDomain_Code\"", "code");
        sql.select(null, "\"PolicyDomain_Name\"", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"PolicyDomain_Name\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"PolicyDomain_Name\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("\"PolicyDomain_Name\"", "ASC"));
        sql.groupBy(new GroupByBean("\"PolicyDomain_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyDomain_Code\""));

        return sql;
    }

    public static SQLBean getMeasureTypes(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "\"PolicyMeasureType_Code\"", "code");
        sql.select(null, "\"PolicyMeasureType_Name\" || ' [' || \"PolicyDomain_Name\" || ']'", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"PolicyMeasureType_Name\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"PolicyMeasureType_Name\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("\"PolicyDomain_Name\"", "ASC"));
        sql.orderBy(new OrderByBean("\"PolicyMeasureType_Name\"", "ASC"));
        sql.groupBy(new GroupByBean("\"PolicyDomain_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyMeasureType_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyMeasureType_Code\""));


        return sql;
    }


    public static SQLBean getPolicyMeasures(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "\"PolicyMeasure_Code\"", "code");
        sql.select(null, "\"PolicyMeasure_Name\" || ' [' || \"PolicyDomain_Name\" || ':' || \"PolicyMeasureType_Name\" || ']'", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"PolicyMeasure_Name\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"PolicyMeasure_Name\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("\"PolicyDomain_Name\"", "ASC"));
        sql.orderBy(new OrderByBean("\"PolicyMeasureType_Name\"", "ASC"));
        sql.orderBy(new OrderByBean("\"PolicyMeasure_Name\"", "ASC"));
        sql.groupBy(new GroupByBean("\"PolicyDomain_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyMeasureType_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyMeasure_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyMeasure_Code\""));

        return sql;
    }


    public static SQLBean getSubPolicyMeasures(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "\"SubPolicyMeasure_Code\"", "code");
        sql.select(null, "\"SubPolicyMeasure_Name\" || ' [' || \"PolicyDomain_Name\" || ':' || \"PolicyMeasureType_Name\" || ':' || \"PolicyMeasure_Name\" || ']' ", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"SubPolicyMeasure_Name\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"SubPolicyMeasure_Name\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("\"PolicyDomain_Name\"", "ASC"));
        sql.orderBy(new OrderByBean("\"PolicyMeasureType_Name\"", "ASC"));
        sql.orderBy(new OrderByBean("\"PolicyMeasure_Name\"", "ASC"));
        sql.orderBy(new OrderByBean("\"SubPolicyMeasure_Name\"", "ASC"));

        sql.groupBy(new GroupByBean("\"PolicyDomain_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyMeasureType_Name\""));
        sql.groupBy(new GroupByBean("\"PolicyMeasure_Name\""));
        sql.groupBy(new GroupByBean("\"SubPolicyMeasure_Name\""));
        sql.groupBy(new GroupByBean("\"SubPolicyMeasure_Code\""));


        return sql;
    }


    public static SQLBean getCommodities(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();
        sql.select(null, "\"Product_AMIS_Code\"", "code");
        sql.select(null, "\"Product_AMIS_Name\"", "label");

        sql.from(table, "D");
        sql.where(SQL.DATE.name(), "\"Product_AMIS_Name\"", "is not", "null", null);
        sql.where(SQL.DATE.name(), "\"Product_AMIS_Name\"", "!=", "''", null);

        for(WhereBean whereBean : whereBeans ) {
            sql.where(whereBean);
        }

        sql.orderBy(new OrderByBean("\"Product_AMIS_Name\"", "ASC"));
        sql.groupBy(new GroupByBean("\"Product_AMIS_Name\""));
        sql.groupBy(new GroupByBean("\"Product_AMIS_Code\""));

        return sql;
    }

    public static SQLBean getCommoditiesHierarchyOriginal(String table, List<WhereBean> whereBeans) {
        SQLBean sql = new SQLBean();

        StringBuilder sb = new StringBuilder();

        ArrayList<String> commodities = new ArrayList<String>();
        commodities.add("Maize");  //Maize
        commodities.add("Rice");   //Rice
        commodities.add("Soybeans"); //Soybeans
        commodities.add("Wheat");  //Wheat


        int i = 0;
        for(String commodityName: commodities){
            sb.append("( ");
            sb.append("SELECT DISTINCT(\"Product_AMIS_Code\"), \"Product_AMIS_Name\", ");
            sb.append("CASE WHEN \"Product_AMIS_Name\" = ('"+commodityName+"') THEN -1 ");
            sb.append("ELSE (SELECT \"Product_AMIS_Code\" FROM "+table+" WHERE \"Product_AMIS_Name\" = ('"+commodityName+"') LIMIT 1) ");
            sb.append("END AS parent_id ");
            sb.append("FROM "+table+" ");
            sb.append("WHERE (\"Product_AMIS_Name\" LIKE '"+commodityName+"%') OR  (\"Product_AMIS_Name\" LIKE '%+ "+commodityName+"%') ORDER BY \"Product_AMIS_Name\" ");
            sb.append(") ");

            if(i < commodities.size()-1)
                sb.append("UNION ALL");

            i++;
        }

        sql.setQuery(sb.toString());

        System.out.println("getCommoditiesHierarchy--- : "+ sql.getQuery());


        return sql;

    }


    public static SQLBean getCommoditiesHierarchy(String table, String countrycodes, String commoditycodes, String policydomaincodes, String measuretypecodes, String policymeasurecodes, String subpolicymeasurecodes) {
        SQLBean sql = new SQLBean();

        StringBuilder sb = new StringBuilder();

        ArrayList<String> commodities = new ArrayList<String>();
        commodities.add("Maize");  //Maize
        commodities.add("Rice");   //Rice
        commodities.add("Soybeans"); //Soybeans
        commodities.add("Wheat");  //Wheat


        sb.append("(");
        sb.append("SELECT DISTINCT(\"Product_AMIS_Code\"), \"Product_AMIS_Name\",  -1 as parent_id ");
        sb.append("FROM "+table+" ");
        sb.append("WHERE ");
        int k = 0;
        sb.append("( ");
        for(String commodityName: commodities){
            sb.append("\"Product_AMIS_Name\" NOT LIKE '%"+commodityName+"%' ");

            if(k < commodities.size()-1)
                sb.append("AND ");

            k++;

        }
        sb.append(") ");
        sb.append("AND ");
        sb.append(addConditions(countrycodes, commoditycodes, policydomaincodes,measuretypecodes, policymeasurecodes, subpolicymeasurecodes, false, table));
        sb.append("ORDER BY \"Product_AMIS_Name\" ");
        sb.append(") ");

        sb.append("UNION ALL");


        int i = 0;
        for(String commodityName: commodities){
            sb.append("( ");
            sb.append("SELECT DISTINCT(\"Product_AMIS_Code\"), \"Product_AMIS_Name\", ");
            sb.append("CASE WHEN \"Product_AMIS_Name\" = ('"+commodityName+"') THEN -1 ");
            sb.append("ELSE (SELECT \"Product_AMIS_Code\" FROM "+table+" WHERE \"Product_AMIS_Name\" = ('"+commodityName+"') LIMIT 1) ");
            sb.append("END AS parent_id ");
            sb.append("FROM "+table+" ");
            sb.append("WHERE (\"Product_AMIS_Name\" LIKE '"+commodityName+"%' OR  \"Product_AMIS_Name\" LIKE '%+ "+commodityName+"%') AND ");
            sb.append(addConditions(countrycodes, commoditycodes, policydomaincodes,measuretypecodes, policymeasurecodes, subpolicymeasurecodes, false, table));
            sb.append("ORDER BY \"Product_AMIS_Name\" ");
            sb.append(") ");

            if(i < commodities.size()-1)
                sb.append("UNION ALL");

            i++;
        }

        sql.setQuery(sb.toString());

        System.out.println("getCommoditiesHierarchyFiltered--- : "+ sql.getQuery());


        return sql;

    }



    public static SQLBean getSummary(String table, String countrycodes, String commoditycodes, String policydomaincodes, String measuretypecodes, String policymeasurecodes, String subpolicymeasurecodes) {
        SQLBean sql = new SQLBean();

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT \"CPL_ID\", \"Country_Name\", \"Product_AMIS_Name\", ");
        sb.append("\"PolicyDomain_Name\" , \"PolicyMeasureType_Name\" , \"PolicyMeasure_Name\" ");

        // FROM
        sb.append("FROM "+table+ " ");

        // WHERES
        sb.append("WHERE ");

        // JOINS
        sb.append(addConditions(countrycodes, commoditycodes, policydomaincodes, measuretypecodes, policymeasurecodes, subpolicymeasurecodes, false, table));

        //GROUP BYS
        sb.append("GROUP BY \"CPL_ID\", \"Country_Name\", \"Product_AMIS_Name\", \"PolicyDomain_Name\", \"PolicyMeasureType_Name\", \"PolicyMeasure_Name\" ");

        // ORDER BYS
        sb.append("ORDER BY \"Country_Name\", \"Product_AMIS_Name\" ");

        sql.setQuery(sb.toString());

        System.out.println(sql.getQuery());

        return sql;
    }

    public static String getSummaryCPL_IDs(String table, String countrycodes, String commoditycodes, String policydomaincodes, String measuretypecodes, String policymeasurecodes, String subpolicymeasurecodes) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT \"CPL_ID\" ");

        // FROM
        sb.append("FROM "+table+ " ");

        // WHERES
        sb.append("WHERE ");

        // JOINS
        sb.append(addConditions(countrycodes, commoditycodes, policydomaincodes, measuretypecodes, policymeasurecodes, subpolicymeasurecodes, false, table));


        return sb.toString();
    }

    public static SQLBean gePolicyValues(String table, String master_table, String countrycodes, String commoditycodes, String policydomaincodes, String measuretypecodes, String policymeasurecodes, String subpolicymeasurecodes) {
        SQLBean sql = new SQLBean();

        StringBuilder sb = new StringBuilder();

        sb.append("SELECT V.\"CPL_ID\", \"Start_Date\" , \"End_Date\", \"Units\", \"Value\",  \"Value_Descr\", ");
        sb.append("V.\"TARGET_HS\", V.\"HS_version\", V.\"HS_suffix\", V.description, \"SubPolicyMeasure_Name\", \"Value_Type\", \"Exemptions_Granted\", \"Commodity_Condition\", ");
        sb.append("\"Subnational_Name\" , \"BechmarkTaxprices\", \"Location_Condition\", ");
        sb.append("\"Value_calculationordirect\", \"Notes\", \"Purpose_Of_Measure\", \"Measure_Description\", \"Source\", \"Title_Of_Notice\", ");
        sb.append("\"Legal_Basis_Name\", \"Sourcehost\", host, leg, agen, impl, \"Start_Date_Comment\" ");
        // FROM
        sb.append("FROM "+table+ " V,  "+master_table + " M ");

        // WHERES
        sb.append("WHERE ");

        // JOINS
        sb.append(addConditions(countrycodes, commoditycodes, policydomaincodes, measuretypecodes, policymeasurecodes, subpolicymeasurecodes, true, master_table));
        sb.append(" AND V.\"CPL_ID\"=M.\"CPL_ID\" ");

        //GROUP BYS
        sb.append("GROUP BY V.\"CPL_ID\", \"Start_Date\" , \"End_Date\", \"Units\", \"Value\",  \"Value_Descr\", ");
        sb.append("V.\"TARGET_HS\", V.\"HS_version\", V.\"HS_suffix\", V.description, \"SubPolicyMeasure_Name\", \"Value_Type\", \"Exemptions_Granted\", \"Commodity_Condition\", ");
        sb.append("\"Subnational_Name\" , \"BechmarkTaxprices\", \"Location_Condition\", ");
        sb.append("\"Value_calculationordirect\", \"Notes\", \"Purpose_Of_Measure\", \"Measure_Description\", \"Source\", \"Title_Of_Notice\", ");
        sb.append("\"Legal_Basis_Name\", \"Sourcehost\", host, leg, agen, impl, \"Start_Date_Comment\" ");


        // ORDER BYS
        sb.append("ORDER BY \"Start_Date\" ASC ");

        sql.setQuery(sb.toString());

        System.out.println(sql.getQuery());


        return sql;
    }


    private static String addConditions(String countrycodes, String commoditycodes, String policydomaincodes, String measuretypecodes, String policymeasurecodes, String subpolicymeasurecodes, Boolean isValuesQuery, String master_table) {
       StringBuilder sb = new StringBuilder();
        boolean addAND = false;

        if(!isValuesQuery){
        // filters
        if ( countrycodes !=null && !countrycodes.equals("null") ) {
            String[] v = getValues(countrycodes);

            sb.append("\"Country_Code\"  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

            addAND = true;
        }

        if ( commoditycodes !=null && !commoditycodes.equals("null") ) {
            String[] v = getValues(commoditycodes);

            if(addAND){
                sb.append("AND " );
            }

            sb.append("\"Product_AMIS_Code\"  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

            addAND = true;
        }


        if ( policydomaincodes !=null && !policydomaincodes.equals("null")) {
            String[] v = getValues(policydomaincodes);

            if(addAND){
                sb.append("AND " );
            }

            sb.append("\"PolicyDomain_Code\"  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

            addAND = true;
        }


        if ( measuretypecodes !=null && !measuretypecodes.equals("null") ) {
            String[] v = getValues(measuretypecodes);

            if(addAND){
                sb.append("AND " );
            }

            sb.append("\"PolicyMeasureType_Code\"  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

            addAND = true;
        }

         if ( policymeasurecodes !=null && !policymeasurecodes.equals("null") ) {
            String[] v = getValues(policymeasurecodes);

             if(addAND){
                 sb.append("AND " );
             }

            sb.append("\"PolicyMeasure_Code\"  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

             addAND = true;
        }

        if ( subpolicymeasurecodes !=null && !subpolicymeasurecodes.equals("null") ) {
            String[] v = getValues(subpolicymeasurecodes);

            if(addAND){
                sb.append("AND " );
            }

            sb.append("\"SubPolicyMeasure_Code\"  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

            addAND = true;
        }
        }
        if(isValuesQuery){
            // CPL_ID Match
            if(addAND){
                sb.append("AND " );
            }

            sb.append("V.\"CPL_ID\" IN (");

            sb.append(getSummaryCPL_IDs(master_table, countrycodes, commoditycodes, policydomaincodes, measuretypecodes, policymeasurecodes, subpolicymeasurecodes));

            sb.append(") ");

            addAND = true;
        }

      /*  Last version if ( fromdate !=null && !fromdate.equals("null") ) {
            String[] v = getValues(fromdate);

            if(addAND){
                sb.append("AND " );
            }

            sb.append("extract(year from to_date(\"Start_Date\", 'DD-MM-YYYY'))  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

            addAND = true;
        }

        if ( todate !=null && !todate.equals("null") ) {
            String[] v = getValues(todate);

            if(addAND){
                sb.append("AND " );
            }

            sb.append("extract(year from to_date(\"End_Date\", 'DD-MM-YYYY'))  IN (" );
            for( int i=0; i < v.length; i++) {
                sb.append("'"+ v[i]+"'");
                if ( i < v.length -1)
                    sb.append(",");
            }
            sb.append( ") ");

            addAND = true;
        }
            */

       /** if ( fromdate !=null && !fromdate.equals("null") && todate !=null && !todate.equals("null")  ) {
            sb.append("AND extract(year from to_date(\"Start_Date\", 'DD-MM-YYYY')) = '" + fromdate + "' ");
            sb.append("AND extract(year from to_date(\"End_Date\", 'DD-MM-YYYY')) = '" + todate + "' ");
           // sb.append("AND extract(year from to_date(\"Start_Date\", 'DD-MM-YYYY')) >= '" + fromdate + "' ");
          //  sb.append("AND extract(year from to_date(\"End_Date\", 'DD-MM-YYYY')) <= '" + todate + "' ");
        }

        if ( fromdate !=null && !fromdate.equals("null") && todate ==null && todate.equals("null")  ) {
            sb.append("AND extract(year from to_date(\"Start_Date\", 'DD-MM-YYYY'))  = '" + fromdate + "' ");
        }

        if ( todate !=null && !todate.equals("null")  && fromdate ==null && fromdate.equals("null")) {
            sb.append("AND extract(year from to_date(\"End_Date\", 'DD-MM-YYYY'))  = '" + todate + "' ");
        }    **/


        return sb.toString();
    }


    private static String[] getValues(String values) {
        return extractValues(values, ",");
    }

    public static String[] extractValues(String s, String separator) {
        String[] tokens = s.split(separator);
        String[] l = new String[tokens.length];
        for (int i = 0 ; i < tokens.length; i++) {
            if  ( !tokens[i].equals("null") ) {
                    l[i] = tokens[i];
            }
            else {
                return null;
            }
        }
        return l;
    }


}