package org.fao.fenix.wds.core.policy;

import org.fao.fenix.wds.core.bean.DatasourceBean;
import org.fao.fenix.wds.core.exception.WDSException;
import org.fao.fenix.wds.core.jdbc.JDBCIterable;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCIterablePolicy extends JDBCIterable {

    public void queryInsert(DatasourceBean db, String sql) throws Exception {
        switch (db.getDriver()) {
            case POSTGRESQL     : queryPostgreSQLInsert(db, sql); break;
            //case SQLSERVER2000  : querySQLServer(db, sql); break;
            default             : throw new WDSException(db.getDriver().name() + " driver has not been implemented (yet).");
        }
    }

    public void queryPostgreSQLInsert(DatasourceBean db, String sql) throws IOException, InstantiationException, SQLException, ClassNotFoundException {

        // Clean the query
        if (!db.getId().equalsIgnoreCase("STAGINGAREA"))
            validate(sql);

        // Open connections
        Class.forName("org.postgresql.Driver");
        this.setConnection(DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword()));
        this.setStatement(this.getConnection().createStatement());
        this.getStatement().executeUpdate(sql);

        //this.setResultSet(this.getStatement().getResultSet());
        try {
            //this.getResultSet().close();
            this.getStatement().close();
            this.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

//        Connection con = null;
//        PreparedStatement pst = null;
//        ResultSet rs = null;
//
////        String url = "jdbc:postgresql://localhost/testdb";
////        String user = "user12";
////        String password = "34klq*";
//
//        System.out.println(sql);
//
//        try {
//
//            con = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());
//            pst = con.prepareStatement(sql);
//            rs = pst.executeQuery();
//
//            while (rs.next()) {
//                System.out.print(rs);
//            }
//
//        } catch (SQLException ex) {
//
//            System.out.println(ex.getMessage());
//
//        } finally {
//
//            try {
//                if (rs != null) {
//                    rs.close();
//                }
//                if (pst != null) {
//                    pst.close();
//                }
//                if (con != null) {
//                    con.close();
//                }
//
//            } catch (SQLException ex) {
//                System.out.println(ex.getMessage());
//            }
//        }
    }

    @Override
    public List<String> next() {

        List<String> l = null;

        if (this.isHasNext()) {
            l = new ArrayList<String>();
            try {
                for (int i = 1 ; i <= this.getResultSet().getMetaData().getColumnCount() ; i++) {
                    try {
                        l.add(this.getResultSet().getString(i).trim());
                    } catch (NullPointerException e) {
                        l.add(this.getResultSet().getString(i));
                    }
                }
                this.setHasNext(this.getResultSet().next());
            } catch(SQLException ignored) {

            }
        }

        if (!this.isHasNext()) {
            try {
                this.getResultSet().close();
                this.getStatement().close();
                this.getConnection().close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return l;
    }

    public void closeConnection(){
        try {
            this.getResultSet().close();
            this.getStatement().close();
            this.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}