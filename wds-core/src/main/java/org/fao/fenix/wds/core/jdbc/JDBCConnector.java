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
package org.fao.fenix.wds.core.jdbc;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.fao.fenix.wds.core.bean.DBBean;
import org.fao.fenix.wds.core.bean.FromBean;
import org.fao.fenix.wds.core.bean.SQLBean;
import org.fao.fenix.wds.core.bean.SelectBean;
import org.fao.fenix.wds.core.constant.DATASOURCE;
import org.fao.fenix.wds.core.constant.SQL;
import org.fao.fenix.wds.core.exception.WDSException;
import org.fao.fenix.wds.core.sql.Bean2SQL;
import org.fao.fenix.wds.core.sql.SQLSplitter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:guido.barbaglia@fao.org">Guido Barbaglia</a>
 * @author <a href="mailto:guido.barbaglia@gmail.com">Guido Barbaglia</a>
 * @author <a href="mailto:cortesini.ivano@fao.org">Ivano Cortesini</a>
 * */
public class JDBCConnector {

    public static List<List<String>> query(DBBean db, SQLBean sql, boolean includeHeaders) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
        List<String> headers = new ArrayList<String>();
        if (includeHeaders) {
            headers = buildHeaders(sql);
        }
        if (sql.getQuery() != null && !sql.getQuery().equals("") && !sql.getQuery().equals(SQL.NONE.name())) {
            if (sql.isSkipCharactersReplacement()) {
                return querySkipCharactersReplacemet(db, sql.getQuery(), headers);
            } else {
                return query(db, sql.getQuery(), headers);
            }
        } else {
            String script = Bean2SQL.convert(sql).toString();
            return query(db, script, headers);
        }
    }

    public static List<List<String>> query(DBBean db, String sql, List<String> headers) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
        switch (db.getDatasource()) {
            case FAOSTAT: return querySQLServer(db, sql, headers);
            case FAOSTAT2: return querySQLServer(db, sql, headers);
            case FAOSTAT3: return querySQLServer(db, sql, headers);
            case FAOSTAT4: return querySQLServer(db, sql, headers);
            case FAOSTATDB: return querySQLServer(db, sql, headers);
            case FAOSTATPROD: return querySQLServer(db, sql, headers);
            case FAOSTATPRODDISS: return querySQLServer(db, sql, headers);
            case FAOSTATTESTDISS: return querySQLServer(db, sql, headers);
            case FAOSTATGLBL: return querySQLServer(db, sql, headers);
            case FAOSYB: return queryPostgreSQL(db, sql, headers);
            case FENIXDATAMANAGER: return queryPostgreSQL(db, sql, headers);
            case CROWDPRICES: return queryPostgreSQL(db, sql, headers);
            case FENIX: return queryPostgreSQL(db, sql, headers);
            case STAGINGAREA: return queryPostgreSQL(db, sql, headers);
            case AMISPOLICIES: return queryPostgreSQL(db, sql, headers);
            default: throw new WDSException(db.getDatasource().name() + " is not recognized as datasource (yet).");
        }
    }

    public static List<List<String>> querySQLServer(DBBean db, String sql, List<String> headers) throws IllegalAccessException, InstantiationException, SQLException {
//        System.out.println(sql);
        validate(sql);
        sql = sql.replaceAll("-", "_");
        sql = sql.replaceAll("/", "_");
        boolean isFAOSTAT = (db.getDatasource() == DATASOURCE.FAOSTAT) ||
                            (db.getDatasource() == DATASOURCE.FAOSTATGLBL) ||
                            (db.getDatasource() == DATASOURCE.FAOSTAT2) ||
                            (db.getDatasource() == DATASOURCE.FAOSTAT3) ||
                            (db.getDatasource() == DATASOURCE.FAOSTAT4) ||
                            (db.getDatasource() == DATASOURCE.FAOSTATDB) ||
                            (db.getDatasource() == DATASOURCE.FAOSTATPROD) ||
                            (db.getDatasource() == DATASOURCE.FAOSTATTESTDISS) ||
                            (db.getDatasource() == DATASOURCE.FAOSTATPRODDISS);
        if (sql.contains("*") && isFAOSTAT) {	// support for SQL's 'SELECT *'
            List<FromBean> froms = SQLSplitter.parseFroms(sql);
            List<String> tableNames = new ArrayList<String>();
            for (FromBean b : froms)
                tableNames.add(b.getColumn());
            List<String> columnNames = getFAOSTATColumnNames(tableNames, db.getDatasource());
            headers = columnNames;
        }
        List<List<String>> table = new ArrayList<List<String>>();
        SQLServerDriver.class.newInstance();
        Connection c = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());
        Statement stmt = c.createStatement();
        stmt.executeQuery(sql);
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            List<String> row = new ArrayList<String>();
            for (int i = 1 ; i < Integer.MAX_VALUE ; i++) {
                try {
                    row.add(rs.getString(i).trim());
                } catch (SQLException e) {
                    break;
                } catch (NullPointerException e) {
                    row.add("n.a.");
                }
            }
            table.add(row);
        }
        if (!headers.isEmpty()) {
            table.add(0, headers);
        }
        rs.close();
        stmt.close();
        c.close();
        return table;
    }

    public static List<List<String>> querySkipCharactersReplacemet(DBBean db, String sql, List<String> headers) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
        switch (db.getDatasource()) {
            case FAOSTAT: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTAT2: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTAT3: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTAT4: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTATDB: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTATPROD: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTATPRODDISS: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTATTESTDISS: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSTATGLBL: return querySQLServerSkipCharactersReplacemet(db, sql, headers);
            case FAOSYB: return queryPostgreSQL(db, sql, headers);
            case FENIXDATAMANAGER: return queryPostgreSQL(db, sql, headers);
            case CROWDPRICES: return queryPostgreSQL(db, sql, headers);
            case FENIX: return queryPostgreSQL(db, sql, headers);
            case STAGINGAREA: return queryPostgreSQL(db, sql, headers);
            case AMISPOLICIES: return queryPostgreSQL(db, sql, headers);
            default: throw new WDSException(db.getDatasource().name() + " is not recognized as datasource (yet).");
        }
    }

    public static List<List<String>> querySQLServerSkipCharactersReplacemet(DBBean db, String sql, List<String> headers) throws IllegalAccessException, InstantiationException, SQLException {
        validate(sql);
		/*
		boolean isFAOSTAT = (db.getDatasource() == DATASOURCE.FAOSTAT) ||
							(db.getDatasource() == DATASOURCE.FAOSTATGLBL) ||
							(db.getDatasource() == DATASOURCE.FAOSTATPROD) ||
							(db.getDatasource() == DATASOURCE.FAOSTATPRODDISS);
		if (sql.contains("*") && isFAOSTAT) {
			List<FromBean> froms = SQLSplitter.parseFroms(sql);
			List<String> tableNames = new ArrayList<String>();
			for (FromBean b : froms)
				tableNames.add(b.getColumn());
			List<String> columnNames = getFAOSTATColumnNames(tableNames, db.getDatasource());
			headers = columnNames;
		}
		*/
        List<List<String>> table = new ArrayList<List<String>>();
        SQLServerDriver.class.newInstance();
        Connection c = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());
        Statement stmt = c.createStatement();
        stmt.executeQuery(sql);
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            List<String> row = new ArrayList<String>();
            for (int i = 1 ; i < Integer.MAX_VALUE ; i++) {
                try {
                    row.add(rs.getString(i).trim());
                } catch (SQLException e) {
                    break;
                } catch (NullPointerException e) {
                    row.add("n.a.");
                }
            }
            table.add(row);
        }
        if (!headers.isEmpty()) {
            table.add(0, headers);
        }
        rs.close();
        stmt.close();
        c.close();
        return table;
    }

    public static List<String> buildHeaders(SQLBean sql) {
        List<String> l = new ArrayList<String>();
        if (sql.getQuery() == null || sql.getQuery().equals("") || sql.getQuery().equals(SQL.NONE.name())) {
            for (SelectBean b : sql.getSelects()) {
                if (b.getAlias() != null && !b.getAlias().equals(SQL.NONE.name())) {
                    l.add(b.getAlias());
                } else {
                    l.add(b.getColumn());
                }
            }
        }
        return l;
    }

    public static int crowdPricesInsert(DBBean db, String sql) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        Connection c = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());
        Statement stmt = c.createStatement();
        return stmt.executeUpdate(sql);
    }

    public static List<List<String>> queryPostgreSQL(DBBean db, String sql, List<String> headers) throws IllegalAccessException, InstantiationException, SQLException, ClassNotFoundException {
        long t0 = System.currentTimeMillis();
        if (db.getDatasource() != DATASOURCE.STAGINGAREA)
            validate(sql);
        List<List<String>> table = new ArrayList<List<String>>();
        Class.forName("org.postgresql.Driver");
        Connection c = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());
        Statement stmt = c.createStatement();
        stmt.executeQuery(sql);
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            List<String> row = new ArrayList<String>();
            for (int i = 1 ; i < Integer.MAX_VALUE ; i++) {
                try {
                    row.add(rs.getString(i).trim());
                } catch (SQLException e) {
                    break;
                } catch (NullPointerException e) {
                    row.add("n.a.");
                }
            }
            table.add(row);
        }
        if (!headers.isEmpty()) {
            table.add(0, headers);
        }
        rs.close();
        stmt.close();
        c.close();
        long t1 = System.currentTimeMillis();
//		System.out.println("[FWDS-CORE] - QUERY - POSTGRESQL - " + (t1 - t0) + " - " + FieldParser.parseDate(new Date(), "FENIXAPPS"));
        return table;
    }

    public static List<String> getFAOSTATColumnNames(List<String> tableNames, DATASOURCE datasource) throws IllegalAccessException, InstantiationException, SQLException {
        long t0 = System.currentTimeMillis();
        SQLServerDriver.class.newInstance();
        DBBean db = new DBBean(datasource);
        List<String> names = new ArrayList<String>();
        Connection c = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());
        Statement stmt = c.createStatement();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME IN (");
        for (int i = 0 ; i < tableNames.size() ; i++) {
            sql.append("'").append(tableNames.get(i)).append("'");
            if (i < tableNames.size() - 1)
                sql.append(", ");
        }
        sql.append(") ");
        sql.append("ORDER BY ORDINAL_POSITION ");
        stmt.executeQuery(sql.toString());
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            for (int i = 1 ; i < Integer.MAX_VALUE ; i++) {
                try {
                    names.add(rs.getString(i).trim());
                } catch (SQLException e) {
                    break;
                }
            }
        }
        rs.close();
        stmt.close();
        c.close();
        long t1 = System.currentTimeMillis();
//		System.out.println("[FWDS-CORE] - QUERY - SQLSERVER - " + (t1 - t0) + " - " + FieldParser.parseDate(new Date(), "FENIXAPPS"));
        return names;
    }

    public static void validate(String sql) throws WDSException {
        System.out.println("IN VALIDATE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        if (sql.toUpperCase().contains(SQL.INSERT.name()))
            throw new WDSException(SQL.INSERT.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.UPDATE.name()) && !sql.toUpperCase().contains(SQL.DATEUPDATE.name()))
            throw new WDSException(SQL.UPDATE.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.DELETE.name()))
            throw new WDSException(SQL.DELETE.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.CREATE.name()) && !sql.toUpperCase().contains(SQL.CREATEDDATE.name()))
            throw new WDSException(SQL.CREATE.name() + " is NOT allowed.");
        if (sql.toUpperCase().contains(SQL.DROP.name()))
            throw new WDSException(SQL.DROP.name() + " is NOT allowed.");
    }

}