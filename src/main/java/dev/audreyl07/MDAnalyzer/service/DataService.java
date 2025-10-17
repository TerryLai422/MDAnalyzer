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
    public Object getHistory1(String symbol) {
        String query =
                "SELECT * FROM historical_d WHERE ticker = '" + symbol + "' ORDER BY date ASC;";
        Map<String, Object> map = (Map<String, Object>) questDBService.executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) map.get("response");
        List<Object> list = (List<Object>) response.get("dataset");

        List<Map<String, Object>> listOfMap = new ArrayList<>();
        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            Map<String, Object> m = new HashMap<>();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            LocalDateTime dateTime = LocalDateTime.parse((String) row.get(1), formatter);
            long milliseconds = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            m.put("time", row.get(1).toString().substring(0, 10));
            m.put("open", row.get(2));
            m.put("high", row.get(3));
            m.put("low", row.get(4));
            m.put("close", row.get(5));
            listOfMap.add(m);
        }
        return listOfMap;
    }
    public Object getHistory(String symbol) {
        String query =
                "SELECT * FROM historical_d WHERE ticker = '" + symbol + "' ORDER BY date ASC;";
        Map<String, Object> map = (Map<String, Object>) questDBService.executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) map.get("response");
        List<Object> list = (List<Object>) response.get("dataset");

        List<Object> t = new ArrayList<>();
        List<Object> o = new ArrayList<>();
        List<Object> h = new ArrayList<>();
        List<Object> l = new ArrayList<>();
        List<Object> c = new ArrayList<>();
        List<Object> v = new ArrayList<>();

        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            LocalDateTime dateTime = LocalDateTime.parse((String) row.get(1), formatter);
            long milliseconds = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            t.add(milliseconds);
            o.add(row.get(2));
            h.add(row.get(3));
            l.add(row.get(4));
            c.add(row.get(5));
            v.add(row.get(6));
        }
        Map<String, Object> result = new HashMap<>();
        result.put("s", "ok");
        result.put("t", t);
        result.put("o", o);
        result.put("h", h);
        result.put("l", l);
        result.put("c", c);
        result.put("v", v);
        return result;
    }
    public Object getLineHistory(String symbol) {
        String query =
                "SELECT * FROM historical_d WHERE ticker = '" + symbol + "' ORDER BY date ASC;";
        Map<String, Object> map = (Map<String, Object>) questDBService.executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) map.get("response");
        List<Object> list = (List<Object>) response.get("dataset");

        int i = 0;
        List<Map<String, Object>> listOfMap = new ArrayList<>();
        for (Object obj : list) {
            List<Object> row = (List<Object>) obj;
            Map<String, Object> m = new HashMap<>();
            m.put("time", row.get(1).toString().substring(0, 10));
            m.put("value", row.get(5));
            listOfMap.add(m);
        }
        return listOfMap;
    }
}