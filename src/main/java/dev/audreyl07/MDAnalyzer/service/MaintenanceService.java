package dev.audreyl07.MDAnalyzer.service;

import io.micrometer.common.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class MaintenanceService {
    @Autowired
    QuestDBService questDBService;

    private Map<String, Object> getFalseMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("success", Boolean.FALSE);
        return map;
    }

    public String getLatestDate(String table, String type) {
        String condition;
        if ("analysis_market".equals(table) || "indicator_d_52w".equals(table)) {
            condition = StringUtils.isNotEmpty(type) ? "type = '" + type + "'" : "";
        } else {
            condition = null;
        }
        return questDBService.getLatestDate(table, condition);
    }

    public Map<String, Object> importRawFiles(String type) {
        String table;
        if ("d".equals(type)) {
            table = "historical_raw_d";
        } else if ("etf_d".equals(type)) {
            table = "historical_raw_etf_d";
        } else if ("indices_d".equals(type)) {
            table = "indices_raw_d";
        } else {
            return getFalseMap();
        }
        boolean truncated = questDBService.truncateTable(table);
        if (!truncated) {
            return getFalseMap();
        }
        return questDBService.importFiles(table);
    }


    public Map<String, Object> insertIntoHistorical(String type) {
        String sourceTable;
        String targetTable;
        String query;
        String historicalQuery = """
                INSERT INTO %s
                SELECT\s
                    replace(ticker, '%s', ''),
                    CASE WHEN per = 'D' THEN
                    to_timestamp(date, 'yyyyMMdd')
                    ELSE dateadd('h', -6, to_timestamp(concat(date,'T',time), 'yyyyMMddTHHmmss'))
                    END AS 'date',
                    open,
                    high,
                    low,
                    close,
                    vol
                FROM %s
                WHERE""";

        if ("d".equals(type)) {
            sourceTable = "historical_raw_d";
            targetTable = "historical_d";
            query = String.format(historicalQuery, targetTable, ".US", sourceTable);
        } else if ("etf_d".equals(type)) {
            sourceTable = "historical_raw_etf_d";
            targetTable = "historical_etf_d";
            query = String.format(historicalQuery, targetTable, ".US", sourceTable);
        } else if ("indices_d".equals(type)) {
            sourceTable = "indices_raw_d";
            targetTable = "indices_d";
            query = String.format(historicalQuery, targetTable, "^", sourceTable);
        } else {
            return getFalseMap();
        }
        String latest = questDBService.getLatestDate(targetTable, null);
        System.out.println("Latest:" + latest);
        if (latest == null) {
            return getFalseMap();
        }
        query += "date > '" + latest + "' ORDER BY date, time ASC;";
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> insertIntoIndicator52w(String type) {
        String sourceTable;
        String targetTable;
        if ("d".equals(type)) {
            sourceTable = "historical_d";
            targetTable = "indicator_d_52w";
        } else if ("etf_d".equals(type)) {
            sourceTable = "historical_etf_d";
            targetTable = "indicator_etf_52w";
        } else {
            return getFalseMap();
        }
//        boolean truncated = questDBService.truncateTable(targetTable);
//        if (!truncated) {
//            return getFalseMap();
//        }
        String latest =  getLatestDate(targetTable, "GENERAL");
        System.out.println("Latest:" + latest);
        if (latest == null) {
            return getFalseMap();
        }
        String indicator52wQuery = """
                WITH first_stage AS
                (SELECT
                  'GENERAL' AS type,
                  date,
                  ticker,
                  high,
                  low,
                  close,
                  first_value(close) OVER (
                      PARTITION BY ticker
                      ORDER BY date
                      ROWS 1 PRECEDING EXCLUDE CURRENT ROW
                  ) AS 'previous_close',
                  vol,
                  first_value(vol) OVER (
                      PARTITION BY ticker
                      ORDER BY date
                      ROWS 1 PRECEDING EXCLUDE CURRENT ROW
                  ) AS 'previous_vol',
                  max(high) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW
                  ) AS 'high52w',
                  max(high) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
                  ) AS 'previous_high52w',
                  min(low) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW
                  ) AS 'low52w',
                  min(low) OVER (
                    PARTITION BY ticker
                      ORDER BY date
                      RANGE BETWEEN '365' DAY PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
                  ) AS 'previous_low52w'
                FROM %s)
                INSERT INTO %s
                SELECT
                  type, date, ticker, high, low, close, previous_close, vol, previous_vol,
                  high52w, previous_high52w, (close - high52w)/high52w, low52w, previous_low52w, (close - low52w)/low52w
                FROM first_stage WHERE date > to_date('%s', 'yyyyMMdd')""";

        String query = String.format(indicator52wQuery, sourceTable, targetTable, latest);
        System.out.println("Query:" + query);
//        return getFalseMap();
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> insertIntoAnalysisMarket(String indicatorType) {
        String condition;
        String query;
        if ("high52w".equals(indicatorType)) {
            condition = " type = 'high52w'";
            query = """
                    INSERT INTO analysis_market
                    SELECT
                        'high52w' as 'type',
                        date,
                        count(ticker) as 'total',
                        SUM(CASE WHEN high52w > previous_high52w THEN 1 ELSE 0 END) AS 'count',
                        (SUM(CASE WHEN high52w > previous_high52w THEN 1 ELSE 0 END) * 1.0 / COUNT(ticker)) * 100 AS 'percentage'
                    FROM indicator_d_52w
                    WHERE
                    previous_close <> null""";
        } else if ("low52w".equals(indicatorType)) {
            condition = " type = 'low52w'";
            query = """
                    INSERT INTO analysis_market
                    SELECT
                        'low52w' as 'type',
                        date,
                        count(ticker) as 'total',
                        SUM(CASE WHEN low52w < previous_low52w THEN 1 ELSE 0 END) AS 'count',
                        (SUM(CASE WHEN low52w < previous_low52w THEN 1 ELSE 0 END) * 1.0 / COUNT(ticker)) * 100 AS 'percentage'
                    FROM indicator_d_52w
                    WHERE
                    previous_close <> null""";
        } else {
            return getFalseMap();
        }
        String latest = questDBService.getLatestDate("analysis_market", condition);
        System.out.println("Latest:" + latest);
        if (latest == null) {
            return getFalseMap();
        }
        query += " AND date > to_date('" + latest + "', 'yyyyMMdd')\n";
        query += " ORDER BY type, date ASC;";
        System.out.println("Query:" + query);
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> updateAnalysisMarket(String type) {
        Map<String, Object> result1 = insertIntoIndicator52w(type);
        if (!result1.containsKey("response")) {
            return getFalseMap();
        }
        Map<String, Object> response1 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response1.getOrDefault("ddl", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> result2 = insertIntoAnalysisMarket("high52w");
        if (!result2.containsKey("response")) {
            return getFalseMap();
        }
        Map<String, Object> response2 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response2.getOrDefault("ddl", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> result3 = insertIntoAnalysisMarket("low52w");
        if (!result3.containsKey("response")) {
            return getFalseMap();
        }
        Map<String, Object> response3 = (Map<String, Object>) result1.get("response");
        if (!"OK".equals(response3.getOrDefault("ddl", "FAILURE"))) {
            return getFalseMap();
        }
        Map<String, Object> map = getFalseMap();
        map.put("success", Boolean.TRUE);
        return map;
    }
}