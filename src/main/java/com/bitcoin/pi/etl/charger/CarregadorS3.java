package com.bitcoin.pi.etl.charger;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Map;

public class CarregadorS3 {

    private final S3Client s3;
    private final String bucketClient;

    public CarregadorS3(S3Client s3, String bucketClient) {
        this.s3 = s3;
        this.bucketClient = bucketClient;
    }

    /**
     * Upload de vários arquivos para client/{idEmpresa}/{dd-MM-yyyy}/nome.csv
     */
    public void uploadPorEmpresaEDia(int idEmpresa, LocalDate data, Map<String, String> arquivosNomeParaConteudo) {
        String pasta = String.format("%d/%02d-%02d-%04d/", idEmpresa, data.getDayOfMonth(), data.getMonthValue(), data.getYear());
        for (Map.Entry<String,String> kv : arquivosNomeParaConteudo.entrySet()) {
            String chave = pasta + kv.getKey();
            PutObjectRequest put = PutObjectRequest.builder()
                    .bucket(bucketClient).key(chave).contentType("text/csv").build();
            s3.putObject(put, RequestBody.fromString(kv.getValue(), StandardCharsets.UTF_8));
            System.out.println("Enviado " + chave);
        }
    }

    /**
     * Upload global (erros)
     */
    public void uploadGlobal(String chave, String conteudo) {
        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucketClient).key(chave).contentType("text/csv").build();
        s3.putObject(put, RequestBody.fromString(conteudo, StandardCharsets.UTF_8));
        System.out.println("Enviado global: " + chave);
    }
}
