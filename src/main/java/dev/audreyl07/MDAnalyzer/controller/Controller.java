package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.QuestDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    QuestDBService questDBService;

    @Value("${mdanalyzer.path.historicalDirectoryPath}")
    String historicalDirectoryPath;

    @Value("${mdanalyzer.path.historicalErrorPath}")
    String historicalErrorPath;



    @PostMapping(value = "/import-questdb")
    public ResponseEntity<Object> importQuestDb(@RequestBody Map<String, Object> request) {
        System.out.println("request:" + request);
        questDBService.importFiles("historical_raw_d", historicalDirectoryPath, historicalErrorPath);
        return ResponseEntity.ok().body(Map.of("success", Boolean.TRUE));
    }
}