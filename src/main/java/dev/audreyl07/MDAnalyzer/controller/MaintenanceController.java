package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.MaintenanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/maintenance")
public class MaintenanceController {

    @Autowired
    MaintenanceService maintenanceService;

    @PostMapping(value = "/import-questdb")
    public ResponseEntity<Object> importQuestDb(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        String type = request.getOrDefault("type", "").toString();
        Map<String, Object> result = maintenanceService.importRawFiles(type);
        result.putIfAbsent("success", Boolean.TRUE);
        return ResponseEntity.ok().body(result);
    }

    @PostMapping(value = "/insert-historical")
    public ResponseEntity<Object> insertIntoHistorical(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        String type = request.getOrDefault("type", "").toString();
        Map<String, Object> result = maintenanceService.insertIntoHistorical(type);
        result.putIfAbsent("success", Boolean.TRUE);
        return ResponseEntity.ok().body(result);
    }

    @PostMapping(value = "/insert-52w")
    public ResponseEntity<Object> insertIndicator52w(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        String type = request.getOrDefault("type", "").toString();
        Map<String, Object> result = maintenanceService.insertIntoIndicator52w(type);
        result.putIfAbsent("success", Boolean.TRUE);
        return ResponseEntity.ok().body(result);
    }

    @PostMapping(value = "/insert-analysis-market")
    public ResponseEntity<Object> insertAnalysisMarket(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        String type = request.getOrDefault("type", "").toString();
        Map<String, Object> result = maintenanceService.insertIntoAnalysisMarket(type);
        result.putIfAbsent("success", Boolean.TRUE);
        return ResponseEntity.ok().body(result);
    }

    @PostMapping(value = "/update-52w")
    public ResponseEntity<Object> test(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        String type = request.getOrDefault("type", "").toString();
        Map<String, Object> result = maintenanceService.updateAnalysisMarket(type);
        result.putIfAbsent("success", Boolean.TRUE);
        return ResponseEntity.ok().body(result);
    }

    @PostMapping(value = "/latest")
    public ResponseEntity<Object> getlatest(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        String table = request.getOrDefault("table", "").toString();
        String type = request.getOrDefault("type", "").toString();
        String result = maintenanceService.getLatestDate(table, type);
        Map<String, Object> map = new HashMap<>();
        map.put("success", Boolean.TRUE);
        map.put("latest", result);
        return ResponseEntity.ok().body(map);
    }
}