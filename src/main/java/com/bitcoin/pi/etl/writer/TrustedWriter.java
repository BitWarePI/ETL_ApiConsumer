package com.bitcoin.pi.etl.writer;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TrustedWriter {

    private final S3Client s3;
    private final String bucketTrusted;
    private static final String HEADER = "id_empresa;datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;mac_address\n";

    public TrustedWriter(S3Client s3, String bucketTrusted) {
        this.s3 = s3;
        this.bucketTrusted = bucketTrusted;
    }

    public void escreverTrusted(String chaveTrusted, List<String> linhasValidas) {
        StringBuilder sb = new StringBuilder();
        sb.append(HEADER);
        for (String l : linhasValidas) sb.append(l).append("\n");

        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucketTrusted).key(chaveTrusted).contentType("text/csv").build();

        s3.putObject(put, RequestBody.fromString(sb.toString(), StandardCharsets.UTF_8));
    }

    public void escreverConteudo(String chave, String conteudo) {
        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucketTrusted).key(chave).contentType("text/csv").build();
        s3.putObject(put, RequestBody.fromString(conteudo, StandardCharsets.UTF_8));
    }
}
