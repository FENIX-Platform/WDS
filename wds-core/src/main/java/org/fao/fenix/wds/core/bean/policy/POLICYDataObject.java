package org.fao.fenix.wds.core.bean.policy;

import java.util.LinkedHashMap;

/**
 * @author <a href="mailto:barbara.cintoli@fao.org">Barbara Cintoli</a>
 * @author <a href="mailto:barbara.cintoli@gmail.com">Barbara Cintoli</a>
 */
public class POLICYDataObject {

    private String datasource = "";
    private String policy_domain_code = "";
    private String commodity_domain_code = "";
    private String commodity_class_code = "";
    private String commodity_class_name = "";
    //It's an array
    private String[] policy_type_code;
    //It's an array
    private String[] policy_type_name;

    //It's an array
    private String[] policy_measure_name;
    //It's an array
    private String[] policy_measure_code;

    //It's an array of array
//    private String[][] policyTypesMeasureInfo;
//    private String[][] policyTypesMeasureInfoName;

    private LinkedHashMap<String, LinkedHashMap<String, String>> commodity_list = new LinkedHashMap<String, LinkedHashMap<String, String>>();
    private LinkedHashMap<String, LinkedHashMap<String, String>> policyTypesMeasureInfoName = new LinkedHashMap<String, LinkedHashMap<String, String>>();

    private String country_code = "";
    private String country_name = "";

    private String subnational_code = "";
    private String subnational_name = "";

    private String individualpolicy_code = "";
    private String individualpolicy_name = "";

    private String condition_code = "";
    private String condition_name = "";
    private String yearTab = "";
    private String year_list = "";
    private String start_date = "";
    private String end_date = "";

    private String date_of_publication = "";
    private String unit = "";

    private boolean chart_type = false;
    //It's an array
    private String[] policy_element;

    private String cpl_id = "";

    private String commodity_id = "";

    private String hs_code = "";
    private String hs_version = "";
    private String hs_suffix = "";
    private String description = "";
    private String short_description = "";
    private String shared_group_code = "";

    private LinkedHashMap<String, LinkedHashMap<String, String>> save_fields = new LinkedHashMap<String, LinkedHashMap<String, String>>();
    private String loggedUser = "";
    private String saveAction = "";

    private LinkedHashMap<String, String> subnationalMap = new LinkedHashMap<String, String>();

    private LinkedHashMap<String, String> countryMap = new LinkedHashMap<String, String>();

    private LinkedHashMap<String, LinkedHashMap<String, String>> subnational_for_coutryMap = new LinkedHashMap<String, LinkedHashMap<String, String>>();

    private LinkedHashMap<String, String> subnationalMap_level_3 = new LinkedHashMap<String, String>();

    private LinkedHashMap<String, LinkedHashMap<String, String>> subnational_for_coutryMap_level_3 = new LinkedHashMap<String, LinkedHashMap<String, String>>();

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getPolicy_domain_code() {
        return policy_domain_code;
    }

    public void setPolicy_domain_code(String policy_domain_code) {
        this.policy_domain_code = policy_domain_code;
    }

    public String getCommodity_domain_code() {
        return commodity_domain_code;
    }

    public void setCommodity_domain_code(String commodity_domain_code) {
        this.commodity_domain_code = commodity_domain_code;
    }

    public String getCommodity_class_code() {
        return commodity_class_code;
    }

    public void setCommodity_class_code(String commodity_class_code) {
        this.commodity_class_code = commodity_class_code;
    }

    public String getCommodity_class_name() {
        return commodity_class_name;
    }

    public void setCommodity_class_name(String commodity_class_name) {
        this.commodity_class_name = commodity_class_name;
    }

    public String[] getPolicy_type_code() {
        return policy_type_code;
    }

    public void setPolicy_type_code(String[] policy_type_code) {
        this.policy_type_code = policy_type_code;
    }

    public String[] getPolicy_type_name() {
        return policy_type_name;
    }

    public void setPolicy_type_name(String[] policy_type_name) {
        this.policy_type_name = policy_type_name;
    }

    public String[] getPolicy_measure_code() {
        return policy_measure_code;
    }

    public void setPolicy_measure_code(String[] policy_measure_code) {
        this.policy_measure_code = policy_measure_code;
    }

    public String getCountry_code() {
        return country_code;
    }

    public void setCountry_code(String country_code) {
        this.country_code = country_code;
    }

    public String getCountry_name() {
        return country_name;
    }

    public void setCountry_name(String country_name) {
        this.country_name = country_name;
    }

    public String getYearTab() {
        return yearTab;
    }

    public void setYearTab(String yearTab) {
        this.yearTab = yearTab;
    }

    public String getYear_list() {
        return year_list;
    }

    public void setYear_list(String year_list) {
        this.year_list = year_list;
    }

    public String getStart_date() {
        return start_date;
    }

    public void setStart_date(String start_date) {
        this.start_date = start_date;
    }

    public String getEnd_date() {
        return end_date;
    }

    public void setEnd_date(String end_date) {
        this.end_date = end_date;
    }

    public String getCpl_id() {
        return cpl_id;
    }

    public void setCpl_id(String cpl_id) {
        this.cpl_id = cpl_id;
    }

    public String getCommodity_id() {
        return commodity_id;
    }

    public void setCommodity_id(String commodity_id) {
        this.commodity_id = commodity_id;
    }

    public String[] getPolicy_element() {return policy_element;}

    public void setPolicy_element(String[] policy_element) {this.policy_element = policy_element;}

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public boolean getChartType() {
        return chart_type;
    }

    public void setChartType(boolean chart_type) {
        this.chart_type = chart_type;
    }

    public LinkedHashMap<String, String> getSubnationalMap() {
        return subnationalMap;
    }

    public void setSubnationalMap(LinkedHashMap<String, String> subnationalMap) {
        this.subnationalMap = subnationalMap;
    }

    public LinkedHashMap<String, LinkedHashMap<String, String>> getSubnational_for_coutryMap() {
        return subnational_for_coutryMap;
    }

    public void setSubnational_for_coutryMap(LinkedHashMap<String, LinkedHashMap<String, String>> subnational_for_coutryMap) {
        this.subnational_for_coutryMap = subnational_for_coutryMap;
    }

    public LinkedHashMap<String, LinkedHashMap<String, String>> getSubnational_for_coutryMap_level_3() {
        return subnational_for_coutryMap_level_3;
    }

    public void setSubnational_for_coutryMap_level_3(LinkedHashMap<String, LinkedHashMap<String, String>> subnational_for_coutryMap_level_3) {
        this.subnational_for_coutryMap_level_3 = subnational_for_coutryMap_level_3;
    }

    public LinkedHashMap<String, String> getSubnationalMap_level_3() {
        return subnationalMap_level_3;
    }

    public void setSubnationalMap_level_3(LinkedHashMap<String, String> subnationalMap_level_3) {
        this.subnationalMap_level_3 = subnationalMap_level_3;
    }

    public LinkedHashMap<String, String> getCountryMap() {
        return countryMap;
    }

    public void setCountryMap(LinkedHashMap<String, String> countryMap) {
        this.countryMap = countryMap;
    }

    public String[] getPolicy_measure_name() {
        return policy_measure_name;
    }

    public void setPolicy_measure_name(String[] policy_measure_name) {
        this.policy_measure_name = policy_measure_name;
    }

//    public String[][] getPolicyTypesMeasureInfo() {
//        return policyTypesMeasureInfo;
//    }
//
//    public void setPolicyTypesMeasureInfo(String[][] policyTypesMeasureInfo) {
//        this.policyTypesMeasureInfo = policyTypesMeasureInfo;
//    }
//
//    public String[][] getPolicyTypesMeasureInfoName() {
//        return policyTypesMeasureInfoName;
//    }
//
//    public void setPolicyTypesMeasureInfoName(String[][] policyTypesMeasureInfoName) {
//        this.policyTypesMeasureInfoName = policyTypesMeasureInfoName;
//    }

    public LinkedHashMap<String, LinkedHashMap<String, String>> getPolicyTypesMeasureInfoName() {
        return policyTypesMeasureInfoName;
    }

    public void setPolicyTypesMeasureInfoName(LinkedHashMap<String, LinkedHashMap<String, String>> policyTypesMeasureInfoName) {
        this.policyTypesMeasureInfoName = policyTypesMeasureInfoName;
    }

    public String getSubnational_code() {
        return subnational_code;
    }

    public void setSubnational_code(String subnational_code) {
        this.subnational_code = subnational_code;
    }

    public String getSubnational_name() {
        return subnational_name;
    }

    public void setSubnational_name(String subnational_name) {
        this.subnational_name = subnational_name;
    }

    public String getCondition_code() {
        return condition_code;
    }

    public void setCondition_code(String condition_code) {
        this.condition_code = condition_code;
    }

    public String getCondition_name() {
        return condition_name;
    }

    public void setCondition_name(String condition_name) {
        this.condition_name = condition_name;
    }

    public String getIndividualpolicy_code() {
        return individualpolicy_code;
    }

    public void setIndividualpolicy_code(String individualpolicy_code) {
        this.individualpolicy_code = individualpolicy_code;
    }

    public String getIndividualpolicy_name() {
        return individualpolicy_name;
    }

    public void setIndividualpolicy_name(String individualpolicy_name) {
        this.individualpolicy_name = individualpolicy_name;
    }

    public String getCommodityclass_code() {
        return commodity_class_code;
    }

    public void setCommodityclass_code(String commodityclass_code) {
        this.commodity_class_code = commodityclass_code;
    }

    public String getDate_of_publication() {
        return date_of_publication;
    }

    public void setDate_of_publication(String date_of_publication) {
        this.date_of_publication = date_of_publication;
    }

    public String getHs_suffix() {
        return hs_suffix;
    }

    public void setHs_suffix(String hs_suffix) {
        this.hs_suffix = hs_suffix;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getShort_description() {
        return short_description;
    }

    public void setShort_description(String short_description) {
        this.short_description = short_description;
    }

    public String getShared_group_code() {
        return shared_group_code;
    }

    public void setShared_group_code(String shared_group_code) {
        this.shared_group_code = shared_group_code;
    }

    public String getHs_code() {
        return hs_code;
    }

    public void setHs_code(String hs_code) {
        this.hs_code = hs_code;
    }

    public String getHs_version() {
        return hs_version;
    }

    public void setHs_version(String hs_version) {
        this.hs_version = hs_version;
    }

    public LinkedHashMap<String, LinkedHashMap<String, String>> getCommodity_list() {
        return commodity_list;
    }

    public void setCommodity_list(LinkedHashMap<String, LinkedHashMap<String, String>> commodity_list) {
        this.commodity_list = commodity_list;
    }

    public LinkedHashMap<String, LinkedHashMap<String, String>> getSave_fields() {
        return save_fields;
    }

    public void setSave_fields(LinkedHashMap<String, LinkedHashMap<String, String>> save_fields) {
        this.save_fields = save_fields;
    }

    public String getLoggedUser() {
        return loggedUser;
    }

    public void setLoggedUser(String loggedUser) {
        this.loggedUser = loggedUser;
    }

    public String getSaveAction() {
        return saveAction;
    }

    public void setSaveAction(String saveAction) {
        this.saveAction = saveAction;
    }
}