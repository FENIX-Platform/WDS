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
package org.fao.fenix.wds.core.utils;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.fao.fenix.wds.core.exception.WDSException;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.UUID;

public class ExcelFactory {

    private String OPEN = "<html><head><title>FENIX Web Data Server</title><link href='axis2-web/css/axis-style.css' rel='stylesheet' type='text/css'/></head><body style='background-color: #A81C7D;'><div style='height: 50px;' ></div><table align='center' style='width: 800px; align: center; border: 3px solid #1D4589; background-color: #FFFFFF;'><tr><td align='left' width='80%' style='font-family: sans-serif; font-weight: bold; color: #A81C7D; font-size: 35pt;'>FENIX Web Data Server</td><td width='200px' align='right'><img src='axis2-web/FAOLogo.png' width='50' height='50' alt='FAO'/></td><td width='200px' align='right'><img src='axis2-web/Axis2Logo.jpg' width='87' height='50' alt='Apache Axis2'/></td></tr><tr><td colspan='3' style='font-family: sans-serif; text-align: justify;'>Your data is available for download by clicking <a href='";

    private String CLOSE = "'>here</a>.<br><br><br></td></tr><tr><td colspan='3' align='center'><a target='_blank' href='http://www.foodsec.org/workstation/'>FENIX Portal</a> | <a target='_blank' href='http://axis.apache.org/axis2/java/core/'>Apache Axis2</a></td></tr></table></body></html>";

    private String ONE = "<HTML><HEAD><meta http-equiv='content-type' content='text/html;charset=utf-8'><TITLE>Redirecting</TITLE><META HTTP-EQUIV='refresh' content='1; url=";

    private String TWO = "'></HEAD><BODY onLoad='location.replace(\"";

    private String THREE = "\")'>Redirecting you...</BODY></HTML>";

    public String createExcel(List<List<String>> table, String pathForExcels, String filename, String ip, String port) throws WDSException {

        try {

            if (filename == null || filename.isEmpty() || filename.startsWith("null.xls"))
                filename = UUID.randomUUID() + ".xls";
            if (!filename.endsWith(".xls"))
                filename += ".xls";

            String fullpath = pathForExcels + File.separator + filename;
            String url = "http://" + ip + ":" + port + "/wds/excels/" + filename;
//            String url = "http://" + ip + ":" + port + "/excels/" + filename;

            Workbook wb = new HSSFWorkbook();
            Sheet s = wb.createSheet("FENIX Web Data Server");

            for (int i = 0; i < table.size(); i++) {
                Row r = s.createRow((short) i);
                for (int j = 0; j < table.get(i).size(); j++) {
                    Cell c = r.createCell(j);
                    try {
                        Double d = Double.valueOf(table.get(i).get(j));
                        c.setCellType(HSSFCell.CELL_TYPE_NUMERIC);
                        c.setCellValue(d);
                    } catch (NumberFormatException e) {
                        c.setCellType(HSSFCell.CELL_TYPE_STRING);
                        c.setCellValue(table.get(i).get(j));
                    }
                }
            }

            FileOutputStream fileOut = new FileOutputStream(fullpath);
            wb.write(fileOut);
            fileOut.close();

//			return OPEN + url + CLOSE;
            return ONE + url + TWO + url + THREE;

        } catch (Exception e) {
            throw new WDSException(e.getMessage());
        }

    }

    public String createExcelForR(List<List<String>> table, String pathForExcels, String filename, String ip, String port) throws WDSException {

        try {

            if (filename == null || filename.isEmpty() || filename.startsWith("null.xls"))
                filename = UUID.randomUUID() + ".xls";
            if (!filename.endsWith(".xls"))
                filename += ".xls";

            String fullpath = pathForExcels + File.separator + filename;
            String url = "http://" + ip + ":" + port + "/r/excel/" + filename;

            Workbook wb = new HSSFWorkbook();
            Sheet s = wb.createSheet("FENIX R");

            for (int i = 0; i < table.size(); i++) {
                Row r = s.createRow((short) i);
                for (int j = 0; j < table.get(i).size(); j++) {
                    Cell c = r.createCell(j);
                    try {
                        Double d = Double.valueOf(table.get(i).get(j));
                        c.setCellType(HSSFCell.CELL_TYPE_NUMERIC);
                        c.setCellValue(d);
                    } catch (NumberFormatException e) {
                        c.setCellType(HSSFCell.CELL_TYPE_STRING);
                        c.setCellValue(table.get(i).get(j));
                    }
                }
            }

            FileOutputStream fileOut = new FileOutputStream(fullpath);
            wb.write(fileOut);
            fileOut.close();

//			return OPEN + url + CLOSE;
            return ONE + url + TWO + url + THREE;

        } catch (Exception e) {
            throw new WDSException(e.getMessage());
        }

    }

}