package com.bitcoin.pi.etl;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ExtratorS3 {

    private final S3Client s3;
    private final String bucket;

    public ExtratorS3(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    public List<String> baixarArquivo(String key) throws Exception {
        return baixarArquivoFromBucket(this.bucket, key);
    }

    public List<String> baixarArquivoFromBucket(String bucket, String key) throws Exception {
        List<String> linhas = new ArrayList<>();
        GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(key).build();

        try (ResponseInputStream<GetObjectResponse> s3Stream = s3.getObject(req);
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Stream, StandardCharsets.UTF_8))) {
            String linha;
            while ((linha = reader.readLine()) != null) linhas.add(linha);
        }
        return linhas;
    }
}
