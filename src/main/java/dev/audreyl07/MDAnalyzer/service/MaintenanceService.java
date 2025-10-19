package dev.audreyl07.MDAnalyzer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class MaintenanceService {
    @Autowired
    QuestDBService questDBService;

    public Map<String, Object> importRawFiles(String type) {
        String table;
        if ("d".equals(type)) {
            table = "historical_raw_d";
        } else if ("etf_d".equals(type)) {
            table = "historical_raw_etf_d";
        } else if ("indices_d".equals(type)) {
            table = "indices_raw_d";
        } else {
            return Map.of("success", Boolean.FALSE);
        }
        boolean truncated = questDBService.truncateTable(table);
        if (!truncated) {
            return Map.of("success", Boolean.FALSE);
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
                    replace(ticker, '%s', ''),\s
                    CASE WHEN per = 'D' THEN\s
                    to_timestamp(date, 'yyyyMMdd')\s
                    ELSE dateadd('h', -6, to_timestamp(concat(date,'T',time), 'yyyyMMddTHHmmss'))\s
                    END AS 'date',\s
                    open,\s
                    high,\s
                    low,\s
                    close,\s
                    vol
                FROM %s
                WHERE\s""";

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
            return Map.of("success", Boolean.FALSE);
        }
        String latest = questDBService.getLatestDate(sourceTable);
        System.out.println("Latest:" + latest);
        query += "date > '" + latest + "' ORDER BY date, time ASC;";
        return questDBService.executeQuery(query);
    }

    public Map<String, Object> insertIndicator52w(String type) {
        String sourceTable;
        String targetTable;
        if ("d".equals(type)) {
            sourceTable = "historical_d";
            targetTable = "indicator_d_52w";
        } else if ("etf_d".equals(type)) {
            sourceTable = "historical_etf_d";
            targetTable = "indicator_etf_52w";
        } else {
            return Map.of("success", Boolean.FALSE);
        }
        boolean truncated = questDBService.truncateTable(targetTable);
        if (!truncated) {
            return Map.of("success", Boolean.FALSE);
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
                FROM first_stage;""";

        String query = String.format(indicator52wQuery, sourceTable, targetTable);
        return questDBService.executeQuery(query);
    }

}