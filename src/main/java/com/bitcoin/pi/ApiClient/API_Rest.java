package com.bitcoin.pi.ApiClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

public class API_Rest {
    public static void ApiConsumer(){
        try {
            // Valores a serem usados
            String email = "bitwarepi@gmail.com";
            String token = "ATATT3xFfGF0SBes5hZbJwMBi1LFyyoofJ0asJgzfl5YS7XnS0Bm0" +
                    "_slNWU7Ud2Qu2yNhuU_n1g5cm-Fr5Yw649uHnkdohw3f7vHTuckP4C3J7" +
                    "5RmUBP4H71KZJUOxyLKUdaZdwAZ49IfrGMLdlRst2KPR4IOPJMWviqcONVPdQpuDV5_bYTciY=7DC91FB6";
            String authentic = email + ":" + token;
            String encodedAuth = Base64.getEncoder().encodeToString(authentic.getBytes());

            // Corpo (json)
            String bodyJson = """
                    {
                      "fields": {
                        "project": { "key": "KAN" },
                        "summary": "Chamado com prioridade máxima.",
                        "description": {
                          "type": "doc",
                          "version": 1,
                          "content": [
                            {
                              "type": "paragraph",
                              "content": [
                                {
                                  "type": "text",
                                  "text": "Chamado criado automaticamente via API Java"
                                }
                              ]
                            }
                          ]
                        },
                        "issuetype": { "id": "10010" },
                        "priority": { "id": "1"},
                        "assignee": { "accountId": "712020:915358ff-e758-48c8-ab6d-5d66f8fe0603" }
                      }
                    }
                    """;

            // Criando Client & Requesição
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://bitwarepi-1760010438510.atlassian.net/rest/api/3/issue"))
                    .header("Authorization", "Basic " + encodedAuth)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(bodyJson))
                    .build();

            // Envia e recebe a resposta
            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

            // Mostra o resultado
            System.out.println("Status Code: " + res.statusCode());
            System.out.println("Response Body: " + res.body());

        } catch (Exception e) {
            System.out.println("Erro na comunicação da API: " + e.getMessage());
        }

    }
}
