package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@CrossOrigin(origins = "http://localhost:1234")
@RestController
@RequestMapping("/stockdata")
public class DataController {

    @Autowired
    DataService dataService;

    @GetMapping(value = "/history")
    public ResponseEntity<Object> getHistory(@RequestParam String symbol) {
        System.out.println("symbol:" + symbol);
        return ResponseEntity.ok().body(dataService.getHistory1(symbol));
    }

    @GetMapping(value = "/full/{symbol}")
    public ResponseEntity<Object> getFullHistory2(@PathVariable String symbol) {
        System.out.println("symbol:" + symbol);
        return ResponseEntity.ok().body(dataService.getHistory(symbol));
    }

    @GetMapping(value = "/line/{symbol}")
    public ResponseEntity<Object> getHistory2(@PathVariable String symbol) {
        System.out.println("symbol:" + symbol);

/*
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(Map.of("time", "2023-01-01", "value", 100));
        list.add(Map.of("time", "2023-01-02", "value", 101));
        list.add(Map.of("time", "2023-01-03", "value", 101));
        list.add(Map.of("time", "2023-01-04", "value", 95));
        list.add(Map.of("time", "2023-01-05", "value", 90));
        list.add(Map.of("time", "2023-01-06", "value", 95));
        list.add(Map.of("time", "2023-01-07", "value", 101));

        return ResponseEntity.ok().body(list);
*/

        return ResponseEntity.ok().body(dataService.getLineHistory(symbol));
    }
}