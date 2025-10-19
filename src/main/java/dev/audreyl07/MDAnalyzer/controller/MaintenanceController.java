package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.MaintenanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
        Map<String, Object> result = maintenanceService.insertIndicator52w(type);
        result.putIfAbsent("success", Boolean.TRUE);
        return ResponseEntity.ok().body(result);
    }
}