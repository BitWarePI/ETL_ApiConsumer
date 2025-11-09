package com.bitcoin.pi.api;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

public class JiraClient {

    private String baseUrl;
    private String encodedAuth;

    public JiraClient(String baseUrl, String email, String token) {
        this.baseUrl = baseUrl;
        String authentic = email + ":" + token;
        this.encodedAuth = Base64.getEncoder().encodeToString(authentic.getBytes());
    }

    public boolean criarCard(String problema, String prioridade) {
        try {
            String prioridadeId = switch (prioridade.toLowerCase()) {
                case "alta" -> "1";
                case "média" -> "2";
                case "baixa" -> "3";
                default -> "3";
            };

            String bodyJson = """
                {
                  "fields": {
                    "project": { "key": "KAN" },
                    "summary": "%s",
                    "description": {
                      "type": "doc",
                      "version": 1,
                      "content": [
                        {
                          "type": "paragraph",
                          "content": [
                            { "type": "text", "text": "%s" }
                          ]
                        }
                      ]
                    },
                    "issuetype": { "id": "10010" },
                    "priority": { "id": "%s" }
                  }
                }
                """.formatted(problema, problema, prioridadeId);

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl))
                    .header("Authorization", "Basic " + encodedAuth)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(bodyJson))
                    .build();

            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            System.out.println("Status Code: " + res.statusCode());
            System.out.println("Response Body: " + res.body());
            return res.statusCode() == 201;

        } catch (Exception e) {
            System.out.println("Erro ao criar card no Jira: " + e.getMessage());
            return false;
        }
    }
}
