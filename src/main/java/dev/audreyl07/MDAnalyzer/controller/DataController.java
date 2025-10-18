package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "http://localhost:1234")
@RestController
@RequestMapping("/stockdata")
public class DataController {

    @Autowired
    DataService dataService;

    @GetMapping(value = "/{type}/{symbol}")
    public ResponseEntity<Object> getHistory2(@PathVariable String type, @PathVariable String symbol) {
        System.out.println("type:" + type);
        System.out.println("symbol:" + symbol);
        return ResponseEntity.ok().body(dataService.getHistoricalData(type, symbol));
    }
}