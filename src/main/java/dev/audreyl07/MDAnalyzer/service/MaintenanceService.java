package dev.audreyl07.MDAnalyzer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class MaintenanceService {
    @Autowired
    QuestDBService questDBService;

    private final String historicalQuery = "INSERT INTO %s\n" +
            "SELECT \n" +
            "    replace(ticker, '%s', ''), \n" +
            "    CASE WHEN per = 'D' THEN \n" +
            "    to_timestamp(date, 'yyyyMMdd') \n" +
            "    ELSE dateadd('h', -6, to_timestamp(concat(date,'T',time), 'yyyyMMddTHHmmss')) \n" +
            "    END AS 'date', \n" +
            "    open, \n" +
            "    high, \n" +
            "    low, \n" +
            "    close, \n" +
            "    vol\n" +
            "FROM %s\n" +
            "WHERE ";

    public Object importRawFiles(String type) {
        String table;
        if ("d".equals(type)) {
            table = "historical_raw_d";
        } else if ("etf_d".equals(type)) {
            table = "historical_raw_etf_d";
        } else if ("indices_d".equals(type)) {
            table = "indices_raw_d";
        } else {
            return Boolean.FALSE;
        }
        boolean truncated = questDBService.truncateTable(table);
        if (!truncated) {
            return Boolean.FALSE;
        }
        questDBService.importFiles(table);
        return Boolean.TRUE;
    }


    public Object insertIntoHistorical(String type) {
        String inTable;
        String outTable;
        String query;
        if ("d".equals(type)) {
            inTable = "historical_d";
            outTable = "historical_raw_d";
            query = String.format(historicalQuery, inTable, ".US", outTable);
        } else if ("etf_d".equals(type)) {
            inTable = "historical_etf_d";
            outTable = "historical_raw_etf_d";
            query = String.format(historicalQuery, inTable, ".US", outTable);
        } else if ("indices_d".equals(type)) {
            inTable = "indices_d";
            outTable = "indices_raw_d";
            query = String.format(historicalQuery, inTable, "^", outTable);
        } else {
            return Boolean.FALSE;
        }
        String latest = questDBService.getLatestDate(inTable);
        System.out.println("Latest:" + latest);
        query += "date > '" + latest + "' ORDER BY date, time ASC;";
        System.out.println("Query:" + query);
        questDBService.executeQuery(query);
        return Boolean.TRUE;
    }
}