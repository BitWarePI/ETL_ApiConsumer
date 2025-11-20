package com.bitcoin.pi.etl;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class ClientWriter {
    private final S3Client s3;
    private final String bucketClient;
    private static final String HEADER = "id_empresa;datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;mac_address\n";

    public ClientWriter(S3Client s3, String bucketClient) {
        this.s3 = s3;
        this.bucketClient = bucketClient;
    }

    public String lerTrusted(String bucketTrusted, String chaveTrusted) {
        GetObjectRequest get = GetObjectRequest.builder()
                .bucket(bucketTrusted)
                .key(chaveTrusted)
                .build();

        InputStream input = s3.getObject(get);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            throw new RuntimeException("Erro ao ler arquivo do trusted!!!!!", e);
        }
    }

    public void escreverClient(String chaveClient, String conteudo) {

        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucketClient)
                .key(chaveClient)
                .contentType("text/csv")
                .build();

        s3.putObject(put, RequestBody.fromString(conteudo, StandardCharsets.UTF_8));
    }

    public void moverTrustedParaClient(String bucketTrusted, String chaveTrusted, String chaveClient) {
        String conteudo = lerTrusted(bucketTrusted, chaveTrusted);
        String processado = processar(conteudo);
        escreverClient(chaveClient, processado);
    }

    private String processar(String conteudo) {
//        return conteudo.lines()
                //A lógica vai estar aquiii!!!!!!
        return null;
    }
}
