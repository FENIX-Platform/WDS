package org.fao.fenix.wds.core.usda;

import org.fao.fenix.wds.core.exception.WDSException;
import org.fao.fenix.wds.core.xml.XMLTools;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:guido.barbaglia@fao.org">Guido Barbaglia</a>
 * @author <a href="mailto:guido.barbaglia@gmail.com">Guido Barbaglia</a> 
 * */
public class USDAClient {

//    private final String WS_URL = "http://www.fas.usda.gov/wsfapsd/svcPSD_AMIS.asmx";

    private final String WS_URL = "http://apps.fas.usda.gov/wsfapsd/svcPSD_AMIS.asmx";

//    private final String ACTION = "http://www.fas.usda.gov/wsfapsd/getDatabyCommodityPerYear";

    private final String ACTION = "http://apps.fas.usda.gov/wsfapsd/getDatabyCommodityPerYear";

    public List<USDABean> getDataByCommodity(String commodityCode, String year, List<String> userCountries, List<String> userAttributes) throws WDSException {
        System.out.println("Processing commodityCode: " + commodityCode);
        try {
            HttpURLConnection connection = buildConnection();
            OutputStream out = connection.getOutputStream();
            Writer wout = new OutputStreamWriter(out);
            writeRequest(wout, commodityCode, year);
            StringBuilder sb = new StringBuilder();
            BufferedReader in = null;
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

        long t0 = System.currentTimeMillis();

        List<USDABean> l = new ArrayList<USDABean>();
        String xml = extractPayload(sb.toString());
        String[] tags = new String[]{"Commodity"};
        List<String> subs = XMLTools.subXML(xml, tags, "getDatabyCommodity");

        for (String sub : subs) {

            USDABean b = parseUSDABean(sub);

            // avoid filters
            if (userCountries == null && userAttributes == null) {

                l.add(b);

            } else {

                if (userAttributes.isEmpty() && userCountries.isEmpty()) {

                    if (USDA.countries.contains(b.getCountryCode()) && USDA.attributes.contains(b.getAttributeID()))
                        l.add(b);
                }

                if (userAttributes.isEmpty() && !userCountries.isEmpty()) {
                    if (userCountries.contains(b.getCountryCode()) && USDA.attributes.contains(b.getAttributeID()))
                        l.add(b);
                }

                if (!userAttributes.isEmpty() && !userCountries.isEmpty()) {
                    if (userCountries.contains(b.getCountryCode()) && userAttributes.contains(b.getAttributeID()))
                        l.add(b);
                }

                if (!userAttributes.isEmpty() && !userCountries.isEmpty()) {
                    if (userCountries.contains(b.getCountryCode()) && userAttributes.contains(b.getAttributeID()))
                        l.add(b);
                }

            }

        }

        long t1 = System.currentTimeMillis();

        return l;
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
        USDABean b = new USDABean();
        b.setAttributeDescription(XMLTools.readChildTag(xml, "Commodity", "Attribute_Description", "getDatabyCommodity").trim());
        b.setAttributeID(XMLTools.readChildTag(xml, "Commodity", "Atrribute_ID", "getDatabyCommodity").trim());
        b.setCalendarYear(XMLTools.readChildTag(xml, "Commodity", "Calendar_Year", "getDatabyCommodity").trim());
        b.setCommodityDescription(XMLTools.readChildTag(xml, "Commodity", "Commodity_Description", "getDatabyCommodity").trim());
        b.setCommodityCode(XMLTools.readChildTag(xml, "Commodity", "Commodity_Code", "getDatabyCommodity").trim());
        b.setCountryCode(XMLTools.readChildTag(xml, "Commodity", "Country_Code", "getDatabyCommodity").trim());
        b.setCountryName(XMLTools.readChildTag(xml, "Commodity", "Country_Name", "getDatabyCommodity").trim());
        b.setMarketYear(XMLTools.readChildTag(xml, "Commodity", "Market_Year", "getDatabyCommodity").trim());
        b.setMonth(XMLTools.readChildTag(xml, "Commodity", "Month", "getDatabyCommodity").trim());
        b.setUnitDescription(XMLTools.readChildTag(xml, "Commodity", "Unit_Description", "getDatabyCommodity").trim());
        b.setUnitID(XMLTools.readChildTag(xml, "Commodity", "Unit_ID", "getDatabyCommodity").trim());
        b.setValue(Double.valueOf(XMLTools.readChildTag(xml, "Commodity", "value", "getDatabyCommodity").trim()));
        return b;
    }

    public String extractPayload(String xml) throws WDSException {
//        System.out.println(xml);
        int idx_1 = xml.indexOf("<getDatabyCommodity ");
        int idx_2 = "</getDatabyCommodity>".length() + xml.indexOf("</getDatabyCommodity>");
        if (idx_1 > -1 && idx_2 > -1)
            return xml.substring(idx_1, idx_2);
        else
            throw new WDSException("Can't extract payload.\n\n\n" + xml);
    }

    private void writeRequest(Writer wout, String commodityCode, String year) throws IOException {

//        wout.write("<soap:Envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:wsf=\"http://www.fas.usda.gov/wsfaspsd/\">");
//        wout.write("<soap:Header/>");
//        wout.write("<soap:Body>");
//        wout.write("<wsf:getDatabyCommodity>");
//        wout.write("<wsf:strCommodityCode>" + commodityCode + "</wsf:strCommodityCode>");
//        wout.write("</wsf:getDatabyCommodity>");
//        wout.write("</soap:Body>");
//        wout.write("</soap:Envelope>");

        wout.write("<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">");
        wout.write("<soap:Body>");
        wout.write("<getDatabyCommodityPerYear xmlns=\"http://www.fas.usda.gov/wsfaspsd/\">");
        wout.write("<strCommodityCode>" + commodityCode + "</strCommodityCode>");
        wout.write("<strYear>" + year + "</strYear>");
        wout.write("</getDatabyCommodityPerYear>");
        wout.write("</soap:Body>");
        wout.write("</soap:Envelope>");

        wout.flush();
        wout.close();

    }

    private HttpURLConnection buildConnection() throws MalformedURLException, IOException {
        URL u = new URL(WS_URL);
        URLConnection uc = u.openConnection();
        HttpURLConnection c = (HttpURLConnection) uc;
        c.setDoOutput(true);
        c.setDoInput(true);
        c.setRequestMethod("POST");
        c.setRequestProperty("action", ACTION);
        c.setRequestProperty("Content-Type", "text/xml");
        c.setRequestProperty("charset", "UTF-8");
        return c;
    }

}
