package org.fao.fenix.wds.core.usda;

import org.apache.log4j.Logger;
import org.fao.fenix.wds.core.exception.WDSException;
import org.fao.fenix.wds.core.xml.XMLTools;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

public class USDAClient {

    private static final Logger LOGGER = Logger.getLogger(USDAClient.class);

    private final String REQUEST_PAYLOAD_COMMODITY = "getDatabyCommodityPerYear";
    private final String REQUEST_PAYLOAD_COMMODITY_WORLD = "getWorldDatabyCommodityPerYear";

    private final String ROOT_XML_WORLD = "getWorldDatabyCommodity";
    private final String ROOT_XML_COMMODITY = "getDatabyCommodity";

    private final String ROOT_BEAN_WORLD = "getWorldDatabyCommodityResult";
    private final String ROOT_BEAN_COMMODITY = "getDatabyCommodity";

    //private final String WS_URL = "https://apps.fas.usda.gov/wsfapsd/svcPSD_AMIS.asmx";
    private final String WS_URL = "https://apps.fas.usda.gov/PSDExternalAPIService/svcPSD_AMIS.asmx";


    private final String ACTION = "https://apps.fas.usda.gov/wsfapsd/getDatabyCommodityPerYear";
    private final String ACTION_WORLD = "https://apps.fas.usda.gov/wsfapsd/getWorldDatabyCommodityPerYear";

    private boolean isWorldService;


    public List<USDABean> getDataByCommodity(String commodityCode, String year, List<String> userCountries, List<String> userAttributes, boolean isWorldService) throws WDSException {
        LOGGER.info("Processing commodityCode: " + commodityCode);

        this.isWorldService = isWorldService;
        try {
            HttpURLConnection connection = buildConnection();
            OutputStream out = connection.getOutputStream();
            Writer wout = new OutputStreamWriter(out);
            writeRequest(wout, commodityCode, year);
            StringBuilder sb = new StringBuilder();
            BufferedReader in = null;
            LOGGER.info("the response of connection is: " + connection.getResponseCode());
            if (connection.getResponseCode() == 200) {

                in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            } else {
                in = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            }
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                sb.append(inputLine);
            }
            in.close();
            return parseUSDAXML(sb, userCountries, userAttributes);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            throw new WDSException(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new WDSException(e.getMessage());
        }
    }


    public String print(List<USDABean> l) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"Cn_code\", \"Country\", \"Cm_code\", \"Commodity\", \"Attribute_id\", \"Attribute\", \"Year\", \"Month\", \"Value\", \"Unit_code\", \"Unit\"\n");
        for (USDABean b : l)
            sb.append(print(b));
        return sb.toString();
    }

    public List<USDABean> parseUSDAXML(StringBuilder sb, List<String> userCountries, List<String> userAttributes) throws WDSException {

        String rootBean = (this.isWorldService) ? ROOT_BEAN_WORLD : ROOT_BEAN_COMMODITY;
        long t0 = System.currentTimeMillis();
        LOGGER.info("start parsing in : " + t0);
        List<USDABean> beans = new ArrayList<USDABean>();
        String xml = extractPayload(sb.toString());

        LOGGER.info("the xml lenght is: " + xml.length());
        String[] tags = new String[]{"Commodity"};
        List<String> subs = XMLTools.subXML(xml, tags, rootBean);

        for (String sub : subs) {

            USDABean b = parseUSDABean(sub);

            // avoid filters
            if (userCountries == null && userAttributes == null) {

                beans.add(b);

            } else {

                if (userAttributes.isEmpty() && userCountries.isEmpty()) {

                    if (USDA.countries.contains(b.getCountryCode()) && USDA.attributes.contains(b.getAttributeID()))
                        beans.add(b);
                }

                if (userAttributes.isEmpty() && !userCountries.isEmpty()) {
                    if (userCountries.contains(b.getCountryCode()) && USDA.attributes.contains(b.getAttributeID()))
                        beans.add(b);
                }

                if (!userAttributes.isEmpty() && !userCountries.isEmpty()) {
                    if (userCountries.contains(b.getCountryCode()) && userAttributes.contains(b.getAttributeID()))
                        beans.add(b);
                }

                if (!userAttributes.isEmpty() && !userCountries.isEmpty()) {
                    if (userCountries.contains(b.getCountryCode()) && userAttributes.contains(b.getAttributeID()))
                        beans.add(b);
                }
            }

        }

        return beans;
    }

    public StringBuilder print(USDABean b) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"").append(b.getCountryCode()).append("\", ");
        sb.append("\"").append(b.getCountryName()).append("\", ");
        sb.append(Long.valueOf(b.getCommodityCode())).append(", ");
        sb.append("\"").append(b.getCommodityDescription()).append("\", ");
        sb.append(Long.valueOf(b.getAttributeID())).append(", ");
        sb.append("\"").append(b.getAttributeDescription()).append("\", ");
        sb.append("\"").append(b.getMarketYear()).append("\", ");
        sb.append("\"").append(b.getMonth()).append("\", ");
        System.out.println("value: " + b.getValue());
        sb.append(Double.valueOf(b.getValue())).append(", ");
        sb.append(Long.valueOf(b.getUnitID())).append(", ");
        sb.append("\"").append(b.getUnitDescription()).append("\"\n");
        return sb;
    }

    public USDABean parseUSDABean(String xml) {

        String rootBean = (this.isWorldService) ? ROOT_BEAN_WORLD : ROOT_BEAN_COMMODITY;
        USDABean b = new USDABean();
        b.setAttributeDescription(XMLTools.readChildTag(xml, "Commodity", "Attribute_Description", rootBean).trim());
        b.setAttributeID(XMLTools.readChildTag(xml, "Commodity", "Attribute_Id", rootBean).trim());
        b.setCalendarYear(XMLTools.readChildTag(xml, "Commodity", "Market_Year", rootBean).trim());
        b.setCommodityDescription(XMLTools.readChildTag(xml, "Commodity", "Commodity_Description", rootBean).trim());
        b.setCommodityCode(XMLTools.readChildTag(xml, "Commodity", "Commodity_code", rootBean).trim());
        b.setCountryCode(XMLTools.readChildTag(xml, "Commodity", "Country_Code", rootBean).trim());
        b.setCountryName(XMLTools.readChildTag(xml, "Commodity", "Country_Name", rootBean).trim());
        b.setMarketYear(XMLTools.readChildTag(xml, "Commodity", "Market_Year", rootBean).trim());
        b.setMonth(XMLTools.readChildTag(xml, "Commodity", "Month", rootBean).trim());
        b.setUnitDescription(XMLTools.readChildTag(xml, "Commodity", "Unit_Description", rootBean).trim());
        b.setUnitID(XMLTools.readChildTag(xml, "Commodity", "Unit_Id", rootBean).trim());
        b.setValue(Double.valueOf(XMLTools.readChildTag(xml, "Commodity", "Value", rootBean).trim()));
        return b;
    }

    public String extractPayload(String xml) throws WDSException {
        System.out.println(xml);
        String root = (this.isWorldService) ? ROOT_XML_WORLD : ROOT_XML_COMMODITY;
        int idx_1 = xml.indexOf("<" + root + " xmlns=\"\">");
        int idx_2 = ("</" + root + ">").length() + xml.indexOf("</" + root + ">");
        if (idx_1 > -1 && idx_2 > -1)
            return xml.substring(idx_1, idx_2);
        else
            throw new WDSException("Can't extract payload.\n\n\n" + xml);
    }

    private void writeRequest(Writer wout, String commodityCode, String year) throws IOException {

        String servicePayload = (this.isWorldService) ? REQUEST_PAYLOAD_COMMODITY_WORLD : REQUEST_PAYLOAD_COMMODITY;
        wout.write("<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">");
        wout.write("<soap:Body>");
        wout.write("<" + servicePayload + " xmlns=\"http://www.fas.usda.gov/wsfaspsd/\">");
        wout.write("<strCommodityCode>" + commodityCode + "</strCommodityCode>");
        wout.write("<strYear>" + year + "</strYear>");
        wout.write("</" + servicePayload + ">");
        wout.write("</soap:Body>");
        wout.write("</soap:Envelope>");

        wout.flush();
        wout.close();

    }

    private HttpURLConnection buildConnection() throws MalformedURLException, IOException {
        String action = (this.isWorldService) ? ACTION_WORLD : ACTION;
        URL u = new URL(WS_URL);
        URLConnection uc = u.openConnection();
        HttpURLConnection c = (HttpURLConnection) uc;
        c.setDoOutput(true);
        c.setDoInput(true);
        c.setRequestMethod("POST");
        c.setRequestProperty("action", action);
        c.setRequestProperty("Content-Type", "text/xml");
        c.setRequestProperty("charset", "UTF-8");
        return c;
    }

}
