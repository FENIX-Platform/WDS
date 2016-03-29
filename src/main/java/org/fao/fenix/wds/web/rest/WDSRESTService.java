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
package org.fao.fenix.wds.web.rest;

import org.fao.fenix.wds.core.bean.*;
import org.fao.fenix.wds.core.constant.DATASOURCE;
import org.fao.fenix.wds.core.constant.OUTPUT;
import org.fao.fenix.wds.core.constant.RESTService;
import org.fao.fenix.wds.core.exception.WDSException;
import org.fao.fenix.wds.core.jdbc.JDBCConnector;
import org.fao.fenix.wds.core.jdbc.WSConnector;
import org.fao.fenix.wds.core.sql.Bean2SQL;
import org.fao.fenix.wds.core.utils.*;
import org.fao.fenix.wds.core.utils.olap.OLAPWrapper;
import org.fao.fenix.wds.web.utils.FAOSYBSQL2;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings("serial")
public class WDSRESTService extends HttpServlet implements Servlet {
	
	private FWDSBean fwds;
	
	private DBBean db;
	
	private SQLBean sql;
	
	private RESTService tableFormat = RESTService.WIDE;
	
	private String language = "EN";
	
	@SuppressWarnings("unchecked")
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		try {
			
			// timing
			long t0 = System.currentTimeMillis();
			
			// prepare output
			String output = parse(request.getParameterMap());
			String filename = filename(request.getParameterMap());
			
			// JSONP callback
			String jsonCallbackParam = request.getParameter("jsoncallback");
			if (jsonCallbackParam != null) {
				if (jsonCallbackParam.equals("?")) {
					output = "callback(" + output + ")";
				} else {
					output = jsonCallbackParam + "(" + output + ")";
				}
			}
			
			// set response
			switch (fwds.getOutput()) {
				case HTML: response.setContentType("text/html"); break;
				case JSON: response.setContentType("application/json"); break;
				case OLAPJSON: 
					response.setContentType("application/json"); 
					if (request.getParameterMap().get("addFlags") != null) {
						boolean addFlags = Boolean.valueOf(request.getParameterMap().get("addFlags").toString());
						fwds.setAddFlags(addFlags);
					}
				break;
				case XML: response.setContentType("application/xml"); break;
				case XML2: response.setContentType("application/xml"); break;
				case CSV: 
					response.setContentType("text/csv");
					response.setHeader("Content-disposition","inline; filename=" + filename + ".csv");
				break;
				case EXCEL: response.setContentType("text/html"); break;
				default: response.setContentType("text/html"); break;
			}
//			response.setContentLength(output.length());
			response.setCharacterEncoding("UTF-8");
			PrintWriter out = response.getWriter();
			out.println(output);
			
			// close stream
			out.close();
			out.flush();
			
			// timing
			long t1 = System.currentTimeMillis();
//			System.out.println("[WDS] - SERVICE - REST - " + (t1 - t0) + " - " + FieldParser.parseDate(new Date(), "FENIXAPPS"));
			
		} catch (InstantiationException e) {
			handleException(response, e.getMessage());
		} catch (IllegalAccessException e) {
			handleException(response, e.getMessage());
		} catch (SQLException e) {
			handleException(response, e.getMessage());
		} catch (ClassNotFoundException e) {
			handleException(response, e.getMessage());
		} catch (WDSException e) {
			handleException(response, e.getMessage());
		}
        
	}
	
	private void handleException(HttpServletResponse response, String message) throws IOException {
		String output = Wrapper.wrapAsHTML(message).toString();
		response.setContentLength(output.length());
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println(output);
		out.close();
		out.flush();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, WDSException {
		doPost(request, response);
	}
	
	private String parse(Map<String,String[]> parameters) throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException, WDSException {
		db = new DBBean();
		fwds = new FWDSBean();
		for(String key: parameters.keySet()) {
			RESTService p = RESTService.valueOf(key.toLowerCase());
			String value = parameters.get(p.name())[0];
			switch (p) {
				case db:
					DATASOURCE datasource = DATASOURCE.valueOf(value.toUpperCase());
					db = new DBBean(datasource);
				break;
				case out:
					OUTPUT output = OUTPUT.valueOf(value.toUpperCase());
					fwds.setOutput(output);
				break;
				case css:
					fwds.setCss(value.toLowerCase());
				break;
				case shownullvalues:
					fwds.setShowNullValues(Boolean.valueOf(value.toLowerCase()));
				break;
			}
		}
		fwds.setDb(db);
		switch (db.getConnection()) {
			case JDBC: return parseJDBC(parameters);
			case WS: return parseWS(parameters);
			default: throw new WDSException(db.getConnection().name() + " is not a valid connection type. Please verify your request.");
		}
	}
	
	private String parseWS(Map<String,String[]> parameters) throws WDSException {
		try {
			switch (db.getDatasource()) {
				case WORLDBANK: return parseWorldBank(parameters);
				case USDA: return parseUSDA(parameters);
				default: throw new WDSException(db.getDatasource().name() + " is not a valid datasource. Please check your request.");
			}
		} catch (Exception e) {
			throw new WDSException(e.getMessage());
		}
	}
	
	private String parseWorldBank(Map<String,String[]> parameters) {
		String worldBankRest = HTTP2WorldBank.convertWorldBank(parameters).toString();
		WBBean selects = HTTP2WorldBank.convertWorldBankSelects(parameters);
		List<List<String>> table = WSConnector.queryWorldBank(worldBankRest, selects); 
		return formatOutput(table, null);
	}
	
	private String parseUSDA(Map<String,String[]> parameters) throws WDSException {
		String filename = !parameters.get("year").equals(null) && parameters.get("year").length >0 ? parameters.get("year")[0]: null;
		List<List<String>> table = WSConnector.queryUSDA(parameters);
		if (table.size() < 2)
			throw new WDSException("No data found. Please check your codes.");
		return formatOutput(table, filename);
	}
	
	private String parseJDBC(Map<String,String[]> parameters) throws IllegalAccessException, InstantiationException, SQLException, WDSException, ClassNotFoundException {
		switch (db.getDatasource()) {
			case FAOSTAT: return parseFAOSTAT(parameters);
            case FAOSTAT2: return parseFAOSTAT(parameters);
            case FAOSTAT3: return parseFAOSTAT(parameters);
            case FAOSTAT4: return parseFAOSTAT(parameters);
            case FAOSTATDB: return parseFAOSTAT(parameters);
			case FAOSTATPROD: return parseFAOSTAT(parameters);
			case FAOSTATPRODDISS: return parseFAOSTAT(parameters);
			case FAOSTATGLBL: return parseFAOSTAT(parameters);
			case FAOSYB: return parseFAOSYB(parameters);
			case FENIXDATAMANAGER: return parseFAOSTAT(parameters);
			case FENIX: return parseFAOSTAT(parameters);
			case CROWDPRICES: return parseFAOSTAT(parameters);
            case STAGINGAREA: return parseStagingArea(parameters);
            case AMISPOLICIES: return parseFAOSTAT(parameters);
            default: throw new WDSException(db.getDatasource().name() + " is not a valid datasource. Please check your request.");
		}
	}
	
	private String parseStagingArea(Map<String,String[]> parameters) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		sql = new SQLBean();
		String tablename = null;
		String values = null;
		String columns = null;
		for(String key: parameters.keySet()) {
			try {
				RESTService p = RESTService.valueOf(key.toLowerCase());
				String value = parameters.get(p.name())[0];
				switch (p) {
					case table: tablename = value; break;
					case values: values = value; break;
					case columns: columns = value; break;
				}
			} catch (IllegalArgumentException e) {
				throw new WDSException(e.getMessage());
			}
		}
		if (tablename == null)
			throw new WDSException("Tablename is null. Please check your syntax.");
		if (values == null)
			throw new WDSException("Tablename is null. Please check your syntax.");
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(tablename).append(" ");
		if (columns != null) {
			StringTokenizer st = new StringTokenizer(columns, ",");
			sb.append("(");
			while (st.hasMoreTokens()) 
				sb.append(st.nextToken()).append(",");
			sb.setCharAt(sb.length() - 1, ' ');
			sb.append(") ");
		}
		sb.append("VALUES (");
		StringTokenizer st1 = new StringTokenizer(values, ",");
		while (st1.hasMoreTokens()) {
			String s = st1.nextToken();
			if (s.contains(":")) {
				StringTokenizer st2 = new StringTokenizer(s, ":");
				Double d = Double.valueOf(st2.nextToken());
				sb.append(d).append(",");
			} else {
				sb.append("'").append(s).append("',");
			}
		}
		sb.setCharAt(sb.length() - 1, ' ');
		sb.append(") ");
		sql.setQuery(sb.toString());
		List<List<String>> table = JDBCConnector.query(db, sql, true);
		return formatOutput(table, null);
	}
	
	private String filename(Map<String,String[]> parameters) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		String f = "FENIX_Web_Data_Server";
		for(String key: parameters.keySet()) {
			RESTService p = RESTService.valueOf(key.toLowerCase());
			String value = parameters.get(p.name())[0];
			switch (p) {
				case object: f = value; break;
			}
		}
		return f;
	}
	
	private String parseFAOSTAT(Map<String,String[]> parameters) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
		sql = new SQLBean();
		String limit = null;
		List<String> headers = new ArrayList<String>();
		try {
			limit = parameters.get("limit")[0];
		} catch (Exception e) {
			
		}
		for(String key: parameters.keySet()) {
			try {
				RESTService p = RESTService.valueOf(key.toLowerCase());
				String value = parameters.get(p.name())[0];
				switch (p) {
					case select: sql.select(CSV2Bean.convertSelect(value)); break;
					case from: sql.from(CSV2Bean.convertFrom(value)); break;
					case where: sql.where(CSV2Bean.convertWhere(value)); break;
					case groupby: sql.groupBy(CSV2Bean.convertGroupBy(value)); break;
					case orderby: sql.orderBy(CSV2Bean.convertOrderBy(value)); break;
//					case limit: sql.setLimit(value); break;
					case frequency: sql.setFrequency(value); break;
				}
			} catch (IllegalArgumentException e) {
//				System.out.println("FWDSRESTService: " + key.toLowerCase());
			}
		}
		fwds.setSql(sql);
		if (limit != null) {
			for (SelectBean sel : sql.getSelects())
				headers.add(sel.getAlias().replaceAll("_", " "));
		}
//		System.out.println("FWDSRESTService @ 3.14 -> " + Bean2SQL.convert(sql));
//		sql.setQuery("SELECT TOP 5 D.GroupCode AS GroupCode, D.GroupNameE AS GroupName " +
//					 "FROM Domain AS D  " +
//					 "GROUP BY D.GroupCode, D.GroupNameE " +
//					 "ORDER BY D.GroupNameE ASC ");
		if (limit != null) {
			String script = Bean2SQL.convert(sql).toString();
			script = script.replaceAll("SELECT ", "SELECT TOP " + limit + " ");
			sql.setQuery(script);
		}
		List<List<String>> table = JDBCConnector.query(db, sql, true);
		if (limit != null) {
			table.add(0, headers);
		}
		return formatOutput(table, null);
	}
		
	public String formatOutput(List<List<String>> table, String filename) {
		String output = "";
		ServletContext servletContext = this.getServletConfig().getServletContext();
		WebApplicationContext wac = WebApplicationContextUtils.getRequiredWebApplicationContext(servletContext);
		Wrapper wrapper = (Wrapper) wac.getBean("wrapper");
		WrapperConfigurations c = new WrapperConfigurations(fwds.getCss());
		switch (fwds.getOutput()) {
			case HTML: 
				output = wrapper.wrapAsHTML(table, true, c).toString();
			break;
			case JSON: output = Wrapper.wrapAsJSON(table).toString(); break;
			case OLAPJSON: 
//				output = OLAPWrapper.wrapAsOLAPJSON(table, fwds.isShowNullValues(), fwds.isAddFlags()).toString(); 
				output = OLAPWrapper.gsonWrapper(table).toString();
			break;
			case CSV: output = wrapper.wrapAsCSV(table, c).toString(); break;
			case EXCEL:
				filename += ".xls";
				output = wrapper.wrapAsExcel(table, filename).toString();
			break;
			case XML2: output = Wrapper.wrapAsXML2(table).toString(); break;
			default: output = Wrapper.wrapAsXML(table).toString(); break;
		}
		return output;
	}
	
	/**
	 * FAO Statistical Yearbook 2012
	 */
	public String parseFAOSYB(Map<String,String[]> parameters) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		String indicatorCode = null;
		String datasetCode = null;
		String objectCode = null;
		String objectSYBCode = null;
		String findCode = null;
		OUTPUT outFormat = OUTPUT.HTML;
		try {
			String lang = parameters.get(RESTService.lang.name())[0];
			language = lang.toUpperCase();
		} catch (NullPointerException e) {
		}
		for(String key: parameters.keySet()) {
			RESTService p = RESTService.valueOf(key.toLowerCase());
			String value = parameters.get(p.name())[0];
			switch (p) {
				case indicator: indicatorCode = value; break;
				case dataset: datasetCode = value; break;
				case object: objectCode = value; break;
				case objectsyb: objectSYBCode = value; break;
				case format: tableFormat = RESTService.valueOf(value.toUpperCase()); break;
				case findcode: findCode = value; break;
				case out: outFormat = OUTPUT.valueOf(value.toUpperCase()); break;
			}
		}
		if (objectCode != null) {
			return queryByObject(objectCode, outFormat);
		} else if (datasetCode != null) {
			return queryByDataset(datasetCode, outFormat);
		} else if (indicatorCode != null) {
			return queryByIndicator(indicatorCode, outFormat);
		} else if (objectSYBCode != null) {
			return queryByObjectSYB(objectSYBCode, outFormat);
		} else if (findCode != null) {
			return queryByFindCode(outFormat);
		} else {
			throw new WDSException("Request is not valid. Please check your syntax.");
		}
	}
	
	private String queryByIndicator(String indicatorCode, OUTPUT outFormat) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		List<List<String>> table = JDBCConnector.query(new DBBean(DATASOURCE.FAOSYB), FAOSYBSQL2.queryByIndicator(indicatorCode, language), FAOSYBSQL2.headers());
		boolean timeseries = indicatorCode.endsWith("_years");
		if (timeseries && tableFormat == RESTService.WIDE) {
			return formatOutput(reshape(table), indicatorCode);
		} else {
			return formatOutput(table, indicatorCode);
		}
	}
	
	private String handleMultipleIndicators(List<List<String>> table, String code) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		List<List<String>> result = new ArrayList<List<String>>();
		for (int i = 0 ; i < table.size() ; i++) {
			String indicatorCode = table.get(i).get(0);
			List<List<String>> tmp = JDBCConnector.query(new DBBean(DATASOURCE.FAOSYB), FAOSYBSQL2.queryByIndicator(indicatorCode, language), new ArrayList<String>());
			for (List<String> row : tmp)
				result.add(row);
		}
		Collections.sort(result, new TableComparator());
		result.add(0, FAOSYBSQL2.headers());
		return formatOutput(result, code);
	}
	
	private String queryByDataset(String datasetCode, OUTPUT outFormat) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		if (datasetCode.equalsIgnoreCase(RESTService.list.name())) {
			List<List<String>> table = JDBCConnector.query(new DBBean(DATASOURCE.FAOSYB), FAOSYBSQL2.listDatasets(language), FAOSYBSQL2.headersForDatasetsList());
			return formatOutput(table, RESTService.list.name());
		} else {
			List<List<String>> table = JDBCConnector.query(new DBBean(DATASOURCE.FAOSYB), FAOSYBSQL2.getIndicatorCodeFromDataset(datasetCode), new ArrayList<String>());
			if (table.size() > 1) {
				return handleMultipleIndicators(table, datasetCode);
			} else {
				String indicatorCode = table.get(0).get(0);
				return queryByIndicator(indicatorCode, outFormat);
			}
		}
	}
	
	private String queryByObject(String objectCode, OUTPUT outFormat) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		List<List<String>> table = JDBCConnector.query(new DBBean(DATASOURCE.FAOSYB), FAOSYBSQL2.getIndicatorCodeFromObject(objectCode), new ArrayList<String>());
		if (table.size() > 1) {
			return handleMultipleIndicators(table, objectCode);
		} else {
			String indicatorCode = table.get(0).get(0);
			return queryByIndicator(indicatorCode, outFormat);
		}
	}
	
	private String queryByObjectSYB(String objectSYBCode, OUTPUT outFormat) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		List<List<String>> table = JDBCConnector.query(new DBBean(DATASOURCE.FAOSYB), FAOSYBSQL2.getIndicatorCodeFromObjectSYB(objectSYBCode), new ArrayList<String>());
		if (table.size() > 1) {
			return handleMultipleIndicators(table, objectSYBCode);
		} else {
			String indicatorCode = table.get(0).get(0);
			return queryByIndicator(indicatorCode, outFormat);
		}
	}
	
	private String queryByFindCode(OUTPUT outFormat) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException, WDSException {
		List<List<String>> table = JDBCConnector.query(new DBBean(DATASOURCE.FAOSYB), FAOSYBSQL2.queryByFindCodeSQL(), FAOSYBSQL2.headersForCodes());
		return formatOutput(table, "CODES");
	}
	
	private String formatMessage(String head, String msg) {
		List<List<String>> t = new ArrayList<List<String>>();
		List<String> h = new ArrayList<String>();
		h.add(head);
		t.add(h);
		List<String> r = new ArrayList<String>();
		r.add(msg);
		t.add(r);
		fwds.setOutput(OUTPUT.HTML);
		return formatOutput(t, null);
	}
	
	public List<List<String>> reshape(List<List<String>> in) {
		List<List<String>> out = new ArrayList<List<String>>();
		List<String> years = getYears(in);
		List<String> countries = getCountries(in);
		List<String> headers = reshapeHeaders(in.get(0), years);
		out.add(headers);
		for (String country : countries)
			out.add(reshapeCountry(in, years, country));
		return out;
	}
	
	private List<String> reshapeCountry(List<List<String>> in, List<String> years, String country) {
		List<String> out = new ArrayList<String>();
		List<List<String>> subIN = filterCountry(in, country);
		for (int i = 0 ; i < subIN.get(0).size() - 2 ; i++)	// copy common part
			out.add(subIN.get(0).get(i));
		for (List<String> row : subIN) {
			for (String year : years) {
				String rowYear = row.get(row.size() - 1);
				if (rowYear.equalsIgnoreCase(year)); {
					out.add(row.get(row.size() - 2));
					break;
				}
			}
		}
		return out;
	}
	
	private List<List<String>> filterCountry(List<List<String>> in, String country) {
		List<List<String>> out = new ArrayList<List<String>>();
		for (int i = 1 ; i < in.size() ; i++) {				// Jump headers row
			List<String> row = in.get(i);
			if (row.get(0).equalsIgnoreCase(country)) {		// Match Country Label
				out.add(row);
			}
		}
		return out;
	}
	
	private List<String> reshapeHeaders(List<String> headers, List<String> years) {
		List<String> l = new ArrayList<String>();
		for (int i = 0 ; i < headers.size() - 2 ; i++)
			l.add(headers.get(i));
		for (String y : years)
			l.add(y);
		return l;
	}
	
	private List<String> getCountries(List<List<String>> in) {
		List<String> countries = new ArrayList<String>();
		for (int i = 1 ; i < in.size() ; i++) {				// Jump headers row
			String y = in.get(i).get(0);					// 0 for Geographic Area Name
			if (!countries.contains(y))						// 1 for Geographic Area Code
				countries.add(y);
		}
		return countries;
	}
	
	private List<String> getYears(List<List<String>> in) {
		List<String> years = new ArrayList<String>();
		int idx = in.get(0).size() - 1;
		for (int i = 1 ; i < in.size() ; i++) {				// Jump headers row
			String y = in.get(i).get(idx);
			if (!years.contains(y))
				years.add(y);
		}
		return years;
	}
	
	public boolean timeseries(List<String> variables) {
		for (String v : variables)
			if (v.trim().endsWith("_years"))
				return true;
		return false;
	}
	
}