package org.fao.fenix.wds.core.usda;

import java.util.ArrayList;
import java.util.List;

public class USDA {

    public static List<String> attributes;

    public static List<String> commodities;

    public static List<String> allCommodities;

    public static List<String> countries;

    static {

        // Attributes
        attributes = new ArrayList<String>();
//		attributes.add("001");						// Area Planted
        attributes.add("004");                        // Area Harvested
        attributes.add("007");                        // Crush
        attributes.add("020");                        // Beginning Stocks
//		attributes.add("022");						// Sow Beginning Stocks
        attributes.add("028");                        // Production
//		attributes.add("054");						// Rough Production
        attributes.add("057");                        // Imports
//		attributes.add("064");						// Raw Imports
//		attributes.add("071");						// Other Imports
        attributes.add("081");                        // TY Imports
        attributes.add("086");                        // Total Supply
        attributes.add("088");                        // Exports
//		attributes.add("113");						// TY Exports
        attributes.add("125");                        // Domestic Consumption
//		attributes.add("126");						// Total Disappearance
//		attributes.add("130");						// Feed Dom. Consumption
//		attributes.add("139");						// Human Dom. Consumption
//		attributes.add("142");						// USE Dom. Consumption
        attributes.add("149");                        // Food Use Dom. Cons.
//		attributes.add("150");						// Loss Dom. Consumption
//		attributes.add("151");						// Other Disappearance
        attributes.add("161");                        // Feed Waste Dom. Cons. (1000 MT)
//		attributes.add("173");						// Total Disappearance
//		attributes.add("174");						// Total Use
        attributes.add("176");                        // Ending Stocks
        attributes.add("178");                        // Total Distribution
//		attributes.add("181");						// Extr. Rate, 999.9999
        attributes.add("182");                        // Milling Rate (.9999)
        attributes.add("184");                        // Yield
        attributes.add("192");                        // FSI Consumption
//		attributes.add("195");						// Stocks-to-Use
//		attributes.add("212");						// Per Capita Consumption
        attributes.add("130");                        // Feed Waste Dom. Cons.
        attributes.add("113");                        // TY Exports

        // Commodities
        commodities = new ArrayList<String>();
        commodities.add("0410000");                    // Wheat
//		commodities.add("0411000");					// Wheat, Durum
        commodities.add("0422110");                    // Rice, Milled
        commodities.add("0440000");                    // Corn
        commodities.add("2222000");                    // Oilseed, Soybean
//        commodities.add("901");					    // Coarse Grains
//        commodities.add("900");					    // Total Cereals
//        commodities.add("903");

        // all commodities
        allCommodities = new ArrayList<String>();
        allCommodities.add("0410000");
//        allCommodities.add("0411000");                // Error
        allCommodities.add("0422110");
        allCommodities.add("0440000");
        allCommodities.add("2222000");
        allCommodities.add("901");
        allCommodities.add("900");
        allCommodities.add("903");
//		allCommodities.add("0011000");					// "Animal Numbers, Cattle"
//		allCommodities.add("0013000");					// "Animal Numbers, Swine"
//		allCommodities.add("0111000");					// "Meat, Beef and Veal"
//		allCommodities.add("0113000");					// "Meat, Swine"
//		allCommodities.add("0114200");					// "Poultry, Meat, Broiler"
//		allCommodities.add("0114300");					// "Poultry, Meat, Turkey"
//		allCommodities.add("0223000");					// "Dairy, Milk, Fluid"
//		allCommodities.add("0224200");					// "Dairy, Milk, Nonfat Dry"
//		allCommodities.add("0224400");					// "Dairy, Dry Whole Milk Powder"
//		allCommodities.add("0230000");					// "Dairy, Butter"
//		allCommodities.add("0240000");					// "Dairy, Cheese"
//		allCommodities.add("0410000");					// Wheat
//		allCommodities.add("0411000");					// "Wheat, Durum"
//		allCommodities.add("0422110");					// "Rice, Milled"
//		allCommodities.add("0430000");					// Barley
//		allCommodities.add("0440000");					// Corn
//		allCommodities.add("0451000");					// Rye
//		allCommodities.add("0452000");					// Oats
//		allCommodities.add("0459100");					// Millet
//		allCommodities.add("0459200");					// Sorghum
//		allCommodities.add("0459900");					// Mixed Grain
//		allCommodities.add("0542100");					// Beans
//		allCommodities.add("0542200");					// Garbanzos
//		allCommodities.add("0542300");					// Lentils
//		allCommodities.add("0542400");					// Peas
//		allCommodities.add("0545900");					// "Asparagus, Fresh"
//		allCommodities.add("0565901");					// "Tomatoes, Canned"
//		allCommodities.add("0565903");					// "Tomato Paste,28-30% TSS Basis"
//		allCommodities.add("0565905");					// Tomato Sauce
//		allCommodities.add("0566100");					// "Potato Products, Frozen"
//		allCommodities.add("0571120");					// "Oranges, Fresh"
//		allCommodities.add("0571220");					// "Tangerines, Fresh"
//		allCommodities.add("0572120");					// "Lemons, Fresh"
//		allCommodities.add("0572220");					// "Grapefruit, Fresh"
//		allCommodities.add("0572920");					// "Citrus, Other, Fresh"
//		allCommodities.add("0574000");					// "Apples, Fresh"
//		allCommodities.add("0575100");					// "Grapes, Table, Fresh"
//		allCommodities.add("0575200");					// Raisins
//		allCommodities.add("0577400");					// "Almonds, Shelled Basis"
//		allCommodities.add("0577500");					// "Filberts, Inshell Basis"
//		allCommodities.add("0577901");					// "Walnuts, Inshell Basis"
//		allCommodities.add("0577903");					// "Pecans, Inshell Basis"
//		allCommodities.add("0577905");					// "Macadamia, Inshell Basis"
//		allCommodities.add("0577907");					// "Pistachios, Inshell Basis"
//		allCommodities.add("0579220");					// "Pears, Fresh"
//		allCommodities.add("0579301");					// Fresh Apricots
//		allCommodities.add("0579305");					// "Fresh Cherries,(Sweet&Sour)"
//		allCommodities.add("0579309");					// Fresh Peaches & Nectarines
//		allCommodities.add("0579311");					// Fresh Plums & Prunes
//		allCommodities.add("0579401");					// "Strawberries, Fresh"
//		allCommodities.add("0579500");					// Kiwifruit
//		allCommodities.add("0579701");					// "Avocados, Fresh"
//		allCommodities.add("0579901");					// "Prunes (Plums, Dried)"
//		allCommodities.add("0585100");					// Orange Juice
//		allCommodities.add("0585120");					// Tangerine Juice
//		allCommodities.add("0585200");					// Grapefruit Juice
//		allCommodities.add("0585300");					// Lemon Juice
//		allCommodities.add("0585700");					// "Apple Juice, Concentrated"
//		allCommodities.add("0586111");					// "Strawberries, Frozen"
//		allCommodities.add("0589901");					// "Peaches, Canned"
//		allCommodities.add("0589903");					// "Pears, Canned"
//		allCommodities.add("0612000");					// "Sugar, Centrifugal"
//		allCommodities.add("0711100");					// "Coffee, Green"
//		allCommodities.add("0813100");					// "Meal, Soybean"
//		allCommodities.add("0813101");					// "Meal, Soybean (Local)"
//		allCommodities.add("0813200");					// "Meal, Peanut"
//		allCommodities.add("0813300");					// "Meal, Cottonseed"
//		allCommodities.add("0813500");					// "Meal, Sunflowerseed"
//		allCommodities.add("0813600");					// "Meal, Rapeseed"
//		allCommodities.add("0813700");					// "Meal, Copra"
//		allCommodities.add("0813710");					// "Meal, Corn Gluten Feed"
//		allCommodities.add("0813800");					// "Meal, Palm Kernel"
//		allCommodities.add("0814200");					// "Meal, Fish"
//		allCommodities.add("1211000");					// "Tobacco, Unmfg., Total"
//		allCommodities.add("1222000");					// "Tobacco, Mfg., Cigarettes"
//		allCommodities.add("2221000");					// "Oilseed, Peanut"
//		allCommodities.add("2222000");					// "Oilseed, Soybean"
//		allCommodities.add("2222001");					// "Oilseed, Soybean (Local)"
//		allCommodities.add("2223000");					// "Oilseed, Cottonseed"
//		allCommodities.add("2224000");					// "Oilseed, Sunflowerseed"
//		allCommodities.add("2226000");					// "Oilseed, Rapeseed"
//		allCommodities.add("2231000");					// "Oilseed, Copra"
//		allCommodities.add("2232000");					// "Oilseed, Palm Kernel"
//		allCommodities.add("2631000");					// Cotton
//		allCommodities.add("4232000");					// "Oil, Soybean"
//		allCommodities.add("4232001");					// "Oil, Soybean (Local)"
//		allCommodities.add("4233000");					// "Oil, Cottonseed"
//		allCommodities.add("4234000");					// "Oil, Peanut"
//		allCommodities.add("4235000");					// "Oil, Olive"
//		allCommodities.add("4236000");					// "Oil, Sunflowerseed"
//		allCommodities.add("4239100");					// "Oil, Rapeseed"
//		allCommodities.add("4242000");					// "Oil, Coconut"
//		allCommodities.add("4243000");					// "Oil, Palm"
//		allCommodities.add("4244000");					// "Oil, Palm Kernel"


        // Countries
        countries = new ArrayList<String>();
        countries.add("AR");                        // Argentina
        countries.add("AS");                        // Australia
        countries.add("BR");                        // Brazil
        countries.add("CA");                        // Canada
        countries.add("EG");                        // Egypt
        countries.add("IN");                        // India
        countries.add("ID");                        // Indonesia
        countries.add("JA");                        // Japan
        countries.add("KZ");                        // Kazakhstan
        countries.add("MX");                        // Mexico
        countries.add("NI");                        // Nigeria
        countries.add("RP");                        // Philippines
        countries.add("SA");                        // Saudi Arabia
        countries.add("SF");                        // South Africa
        countries.add("TH");                        // Thailand
        countries.add("TU");                        // Turkey
        countries.add("UP");                        // Ukraine
        countries.add("CH");                        // China
        countries.add("FR");                        // France
        countries.add("GE");                        // Germany
        countries.add("IT");                        // Italy
        countries.add("KS");                        // Korea, South
        countries.add("RS");                        // Russia
        countries.add("SP");                        // Spain
        countries.add("UK");                        // United Kingdom
        countries.add("US");                        // United States
        countries.add("VM");                        // Vietnam
        countries.add("E4");                        // EU-27
        countries.add("00");                        // World
        countries.add("TW");                        // Taiwan

    }

}