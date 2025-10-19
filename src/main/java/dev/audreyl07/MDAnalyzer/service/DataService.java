package dev.audreyl07.MDAnalyzer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DataService {

    @Autowired
    QuestDBService questDBService;


    public List<Map<String, Object>> getData(String dataType, String resultType, String symbol) {
        String query;
        if ("stock".equals(dataType)) {
            query = "SELECT * FROM historical_d WHERE ticker = '" + symbol + "' ORDER BY date ASC;";
        } else if ("index".equals(dataType)) {
            query =
                    "SELECT * FROM indices_d WHERE ticker = '" + symbol + "' ORDER BY date ASC;";
        } else {
            return List.of();
        }
        Map<String, Object> map = (Map<String, Object>) questDBService.executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) map.get("response");
        List<Object> list = (List<Object>) response.get("dataset");

        if ("full".equalsIgnoreCase(resultType)) {
            return outputAsFull(list);
        }

        return outputAsSingle(list);
    }

    private List<Map<String, Object>> outputAsSingle(List<Object> list) {
        List<Map<String, Object>> listOfMap = new ArrayList<>();
        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            Map<String, Object> m = new HashMap<>();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            LocalDateTime dateTime = LocalDateTime.parse((String) row.get(1), formatter);
            long milliseconds = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            m.put("time", milliseconds / 1000);
            m.put("value", row.get(5));
            m.put("volume", row.get(6));
            listOfMap.add(m);
        }
        return listOfMap;
    }

    private List<Map<String, Object>> outputAsFull(List<Object> list) {
        List<Map<String, Object>> listOfMap = new ArrayList<>();
        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            Map<String, Object> m = new HashMap<>();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            LocalDateTime dateTime = LocalDateTime.parse((String) row.get(1), formatter);
            long milliseconds = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            m.put("time", milliseconds / 1000);
            m.put("open", row.get(2));
            m.put("high", row.get(3));
            m.put("low", row.get(4));
            m.put("close", row.get(5));
            m.put("volume", row.get(6));
            listOfMap.add(m);
        }
        return listOfMap;
    }
}