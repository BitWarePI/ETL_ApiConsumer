package com.bitcoin.pi.etl;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;

public class S3Uploader {
    public static void enviarCsvParaS3(String bucket, String key, String conteudoCsv) {

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("text/csv")
                .build();

        s3.putObject(
                request,
                RequestBody.fromBytes(conteudoCsv.getBytes(StandardCharsets.UTF_8))
        );

        System.out.println("CSV enviado com sucesso!!!!!!!!!");
    }
}
