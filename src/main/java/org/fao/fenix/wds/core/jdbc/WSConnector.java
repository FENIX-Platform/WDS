/**
 * FENIX (Food security and Early warning Network and Information Exchange)
 * <p/>
 * Copyright (c) 2011, by FAO of UN under the EC-FAO Food Security
 * Information for Action Programme
 * <p/>
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.fao.fenix.wds.core.jdbc;

import org.apache.log4j.Logger;
import org.fao.fenix.wds.core.exception.WDSException;
import org.fao.fenix.wds.core.usda.USDA;
import org.fao.fenix.wds.core.usda.USDABean;
import org.fao.fenix.wds.core.usda.USDAClient;
import org.fao.fenix.wds.core.utils.Wrapper;
import org.fao.fenix.wds.core.xml.XMLTools;
import org.fao.fenix.wds.core.bean.WBBean;
import org.fao.fenix.wds.core.constant.RESTService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class WSConnector {

    private static final Logger LOGGER = Logger.getLogger(WSConnector.class);

    public static List<List<String>> queryUSDA(Map<String, String[]> parameters) throws WDSException {

        // init variables
        List<List<String>> table = new ArrayList<List<String>>();
        List<String> commodities = new ArrayList<String>();
        List<String> countries = new ArrayList<String>();
        List<String> attributes = new ArrayList<String>();
        List<USDABean> beans = new ArrayList<USDABean>();
        String year = "2010";
        boolean avoidFilters = false;

        // fetch parameters
        for (String key : parameters.keySet()) {
            RESTService p = RESTService.valueOf(key.toLowerCase());
            String value = parameters.get(p.name())[0];
            switch (p) {
                case year:
                    year = value;
                    break;
                case attribute:
                    attributes = parseCodes(value, 3);
                    break;
                case commodity:
                    commodities = parseCodes(value, 7);
                    break;
                case country:
                    countries = parseCodes(value, 2);
                    break;
                case filters:
                    if (value.equalsIgnoreCase(RESTService.none.name()))
                        avoidFilters = true;
                    break;
            }
        }

        // avoid filters
        if (avoidFilters) {
            countries = null;
            attributes = null;
        }
        LOGGER.info("year? " + year);

        // fetch data
        USDAClient c = new USDAClient();
        if (avoidFilters) {
            LOGGER.info("avoid filter");
            for (String commodityCode : USDA.allCommodities) {
                List<USDABean> tmp = c.getDataByCommodity(commodityCode, year, countries, attributes, false);
                List<USDABean> tmpWorld = c.getDataByCommodity(commodityCode, year, countries, attributes, true);
                beans.addAll(tmp);
                beans.addAll(tmpWorld);

            }
        } else {
            if (commodities.isEmpty()) {
                LOGGER.info("commodities are empty");
                for (String commodityCode : USDA.commodities) {
                    List<USDABean> tmp = c.getDataByCommodity(commodityCode, year, countries, attributes, false);
                    List<USDABean> tmpWorld = c.getDataByCommodity(commodityCode, year, countries, attributes, true);
                    beans.addAll(tmp);
                    beans.addAll(tmpWorld);
                }
            } else {
                LOGGER.info("commodities are NOT empty");
                for (String commodityCode : commodities) {
                    List<USDABean> tmp = c.getDataByCommodity(commodityCode, year, countries, attributes, false);
                    List<USDABean> tmpWorld = c.getDataByCommodity(commodityCode, year, countries, attributes, true);
                    beans.addAll(tmp);
                    beans.addAll(tmpWorld);
                }
            }
        }

        // headers
        List<String> headers = usdaHeaders();
        table.add(headers);
        for (USDABean b : beans)
            table.add(usdaBean2List(b));

        return table;
    }

    private static List<String> usdaBean2List(USDABean b) {
        List<String> l = new ArrayList<String>();
        l.add(b.getCountryCode());
        l.add(b.getCountryName());
        l.add(b.getCommodityCode());
        l.add(b.getCommodityDescription());
        l.add(b.getAttributeID());
        l.add(b.getAttributeDescription());
        l.add(b.getMarketYear());
        l.add(b.getMonth());
        l.add(String.valueOf(b.getValue()));
        l.add(b.getUnitID());
        l.add(b.getUnitDescription());
        return l;
    }

    private static List<String> usdaHeaders() {
        List<String> headers = new ArrayList<String>();
        headers.add("Cn_code");
        headers.add("Country");
        headers.add("Cm_code");
        headers.add("Commodity");
        headers.add("Attribute_id");
        headers.add("Attribute");
        headers.add("Year");
        headers.add("Month");
        headers.add("Value");
        headers.add("Unit_code");
        headers.add("Unit");
        return headers;
    }

    private static List<String> parseCodes(String parameterValue, int codeLenght) throws WDSException {
        List<String> l = new ArrayList<String>();
        if (parameterValue.equalsIgnoreCase(RESTService.all.name())) {
            return l;
        } else {
            StringTokenizer st = new StringTokenizer(parameterValue, ",");
            while (st.hasMoreTokens()) {
                String code = st.nextToken();
                if (code.length() < codeLenght)
                    throw new WDSException("Code '" + code + "' should have " + codeLenght + " digits, please check your selection.");
                else
                    l.add(code);
            }
            return l;
        }
    }

    public static List<List<String>> queryWorldBank(String rest, WBBean selects) throws WDSException {
        try {
            boolean listCountries = listCountries(selects);
            boolean listIndicators = listIndicators(selects);
            URL url = new URL(rest);
            URLConnection conn = url.openConnection();
            conn.setDoOutput(true);
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String tmp = in.readLine();
            String xml = "";
            while (tmp != null) {
                xml += tmp;
                tmp = in.readLine();
            }
            in.close();
            List<WBBean> beans = null;
            if (listCountries) {
                beans = parseWorldBankCountries(xml);
            } else if (listIndicators) {
                beans = parseWorldBankIndicators(xml);
            } else {
                beans = parseWorldBank(xml);
            }
            List<List<String>> table = null;
            if (listCountries) {
                table = Wrapper.wrapWorldBankCountries(beans, selects);
            } else if (listIndicators) {
                table = Wrapper.wrapWorldBankIndicators(beans, selects);
            } else {
                table = Wrapper.wrapWorldBank(beans, selects);
            }
            return table;
        } catch (MalformedURLException e) {
            throw new WDSException(e.getMessage());
        } catch (IOException e) {
            throw new WDSException(e.getMessage());
        }
    }

    public static boolean listCountries(WBBean b) {
        if (b.showCountry() && !b.showDate() && !b.showIndicator() && !b.showValue())
            return true;
        return false;
    }

    public static boolean listIndicators(WBBean b) {
        if (!b.showCountry() && !b.showDate() && b.showIndicator() && !b.showValue())
            return true;
        return false;
    }

    public static List<WBBean> parseWorldBank(String xml) {
        try {
            List<WBBean> beans = new ArrayList<WBBean>();
            int total = Integer.valueOf(XMLTools.readAttribute(xml, "total"));
            for (int i = 0; i < total; i++)
                beans.add(new WBBean());
            List<String> values = XMLTools.readSubCollection(xml, new String[]{"wb:data", "wb:value"}, "wb:data");
            List<String> dates = XMLTools.readSubCollection(xml, new String[]{"wb:data", "wb:date"}, "wb:data");
            List<String> countries = XMLTools.readSubCollection(xml, new String[]{"wb:data", "wb:country"}, "wb:data");
            List<String> indicators = XMLTools.readSubCollection(xml, new String[]{"wb:data", "wb:indicator"}, "wb:data");
            for (int i = 0; i < total; i++) {
                if (!values.isEmpty() && values.get(i) != null && !values.get(i).isEmpty()) {
                    beans.get(i).setValue(Double.valueOf(values.get(i)));
                } else {
                    beans.get(i).setValue("n.a.");
                }
                if (!dates.isEmpty()) {
                    beans.get(i).setDate(dates.get(i));
                } else {
                    beans.get(i).setDate("n.a.");
                }
                if (!countries.isEmpty()) {
                    beans.get(i).setCountryLabel(countries.get(i));
                } else {
                    beans.get(i).setCountryLabel("n.a.");
                }
                if (!indicators.isEmpty()) {
                    beans.get(i).setIndicatorCode(indicators.get(i));
                } else {
                    beans.get(i).setIndicatorCode("n.a.");
                }
            }
            return beans;
        } catch (Exception e) {
            e.printStackTrace();
            throw new WDSException(e.getMessage());
        }
    }

    public static List<WBBean> parseWorldBankCountries(String xml) {
        try {
            List<WBBean> beans = new ArrayList<WBBean>();
            int total = Integer.valueOf(XMLTools.readAttribute(xml, "total"));
            for (int i = 0; i < total; i++)
                beans.add(new WBBean());
            List<String> countries = XMLTools.readSubCollection(xml, new String[]{"wb:country", "wb:name"}, "wb:countries");
            List<String> codes = XMLTools.readSubCollection(xml, new String[]{"wb:country", "wb:iso2Code"}, "wb:countries");
            for (int i = 0; i < total; i++) {
                if (!countries.isEmpty()) {
                    beans.get(i).setCountryLabel(countries.get(i));
                } else {
                    beans.get(i).setCountryLabel("n.a.");
                }
                if (!codes.isEmpty()) {
                    beans.get(i).setCountryCode(codes.get(i));
                } else {
                    beans.get(i).setCountryCode("n.a.");
                }
            }
            return beans;
        } catch (Exception e) {
            e.printStackTrace();
            throw new WDSException(e.getMessage());
        }
    }

    public static List<WBBean> parseWorldBankIndicators(String xml) {
        try {
            List<WBBean> beans = new ArrayList<WBBean>();
            int total = Integer.valueOf(XMLTools.readAttribute(xml, "total"));
            for (int i = 0; i < total; i++)
                beans.add(new WBBean());
            List<String> indicators = XMLTools.readSubCollection(xml, new String[]{"wb:indicator", "wb:name"}, "wb:indicators");
            List<String> codes = XMLTools.readSubCollectionAttribute(xml, new String[]{"wb:indicator"}, "wb:indicators", "id");
            for (int i = 0; i < total; i++) {
                if (!indicators.isEmpty()) {
                    beans.get(i).setIndicatorLabel(indicators.get(i));
                } else {
                    beans.get(i).setIndicatorLabel("n.a.");
                }
                if (!codes.isEmpty()) {
                    beans.get(i).setIndicatorCode(codes.get(i));
                } else {
                    beans.get(i).setIndicatorCode("n.a.");
                }
            }
            return beans;
        } catch (Exception e) {
            e.printStackTrace();
            throw new WDSException(e.getMessage());
        }
    }

}