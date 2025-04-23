package uk.ac.ed.acp.cw2.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/blob")
public class FakeAcpController {

    @PostMapping
    public ResponseEntity<Map<String, String>> storeFakeBlob(@RequestBody Map<String, Object> data) {

        String uuid = UUID.randomUUID().toString();
        Map<String, String> result = new HashMap<>();
        result.put("uuid", uuid);

        System.out.println("======FakeACP======Received blob: " + data);
        System.out.println("======FakeACP======Returning UUID: " + uuid);

        return ResponseEntity.ok(result);
    }
}