package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/mdanalyzer")
public class Controller {

    @Autowired
    DataService dataService;

    @PostMapping(value = "/import-questdb")
    public ResponseEntity<Object> importQuestDb(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        dataService.importRawFiles();
        return ResponseEntity.ok().body(Map.of("success", Boolean.TRUE));
    }

    @PostMapping(value = "/insert-historical")
    public ResponseEntity<Object> insertIntoHistorical(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        dataService.insertIntoHistorical("");
        return ResponseEntity.ok().body(Map.of("success", Boolean.TRUE));
    }
}