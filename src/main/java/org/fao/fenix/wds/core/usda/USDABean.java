package org.fao.fenix.wds.core.usda;

/**
 * @author <a href="mailto:guido.barbaglia@fao.org">Guido Barbaglia</a>
 * @author <a href="mailto:guido.barbaglia@gmail.com">Guido Barbaglia</a> 
 * */
public class USDABean {

	private String commodityCode;

	private String commodityDescription;

	private String countryCode;

	private String countryName;

	private String marketYear;

	private String calendarYear;

	private String month;

	private String attributeID;

	private String attributeDescription;

	private String unitID;

	private String unitDescription;

	private Double value;

	public String getCommodityCode() {
		return commodityCode;
	}

	public void setCommodityCode(String commodityCode) {
		this.commodityCode = commodityCode;
	}

	public String getCommodityDescription() {
		return commodityDescription;
	}

	public void setCommodityDescription(String commodityDescription) {
		this.commodityDescription = commodityDescription;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getCountryName() {
		return countryName;
	}

	public void setCountryName(String countryName) {
		this.countryName = countryName;
	}

	public String getMarketYear() {
		return marketYear;
	}

	public void setMarketYear(String marketYear) {
		this.marketYear = marketYear;
	}

	public String getCalendarYear() {
		return calendarYear;
	}

	public void setCalendarYear(String calendarYear) {
		this.calendarYear = calendarYear;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getAttributeID() {
		return attributeID;
	}

	public void setAttributeID(String attributeID) {
		this.attributeID = attributeID;
	}

	public String getAttributeDescription() {
		return attributeDescription;
	}

	public void setAttributeDescription(String attributeDescription) {
		this.attributeDescription = attributeDescription;
	}

	public String getUnitID() {
		return unitID;
	}

	public void setUnitID(String unitID) {
		this.unitID = unitID;
	}

	public String getUnitDescription() {
		return unitDescription;
	}

	public void setUnitDescription(String unitDescription) {
		this.unitDescription = unitDescription;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Attribute Description: " + this.getAttributeDescription() + ", " +
				"Attribute ID: " + this.getAttributeID() + ", " +
				"Calendar Year: " + this.getCalendarYear() + ", " +
				"Commodity Code: " + this.getCommodityCode() + ", " +
				"Commodity Description: " + this.getCommodityDescription() + ", " +
				"Country Code: " + this.getCountryCode() + ", " +
				"Country Name: " + this.getCountryName() + ", " +
				"Market Year: " + this.getMarketYear() + ", " +
				"Month: " + this.getMonth() + ", " +
				"Unit Description: " + this.getUnitDescription() + ", " +
				"Unit ID: " + this.getUnitID() + ", " +
				"Value: " + this.getValue();
	}

}