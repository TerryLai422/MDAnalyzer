package dev.audreyl07.MDAnalyzer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DataService {
    @Autowired
    QuestDBService questDBService;

    public Object importRawFiles() {
        return questDBService.importFiles("historical_raw_d");
    }

    public Object insertIntoHistorical(String date) {
        String query = "INSERT INTO historical_d\n" +
                "SELECT \n" +
                "    replace(ticker, '.US', ''), \n" +
                "    CASE WHEN per = 'D' THEN \n" +
                "    to_timestamp(date, 'yyyyMMdd') \n" +
                "    ELSE dateadd('h', -6, to_timestamp(concat(date,'T',time), 'yyyyMMddTHHmmss')) \n" +
                "    END AS 'date', \n" +
                "    open, \n" +
                "    high, \n" +
                "    low, \n" +
                "    close, \n" +
                "    vol\n" +
                "FROM historical_raw_d\n" +
                "WHERE ";
        if ("".equals(date)) {
            query += "date >= '19700101' \n";
        } else {
            query += "date > '" + date + "' \n";
        }
        query += "ORDER BY date, time ASC;";
        return questDBService.executeQuery(query);
    }
}