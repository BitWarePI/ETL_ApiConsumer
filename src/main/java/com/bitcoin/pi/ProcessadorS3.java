package com.bitcoin.pi;

import com.bitcoin.pi.ApiClient.API_Rest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ProcessadorS3 {

    private static final int COLUNAS_LEITURA = 9;
    private static final int COLUNAS_PROCESSO = 6;

    public static void main(String[] args) {
        API_Rest.ApiConsumer();
        Region regiao = Region.US_EAST_1;

        String bucketRaw = "s3-raw-bitwarepi";
        String bucketTrusted = "s3-client-bitwarepi";

        String chaveLeituras = "dados/leituras.csv";
        String chaveProcessos = "dados/processos.csv";

        String chaveSaidaTrusted = "dados-enriquecidos/maquinas_com_top10_processos.csv";
        String chaveErrosLeituras = "dados-erros/erros_leituras.csv";
        String chaveErrosProcessos = "dados-erros/erros_processos.csv";

        S3Client s3 = S3Client.builder().region(regiao).build();
        System.out.println("Cliente S3 iniciado.");
        BitwareSQL banco = new BitwareSQL();

        Map<String, LeituraComProcessos> dadosMapeados = new HashMap<>();

        StringBuilder conteudoSaidaFinal = new StringBuilder();
        StringBuilder conteudoErrosLeituras = new StringBuilder();
        StringBuilder conteudoErrosProcessos = new StringBuilder();

        conteudoErrosLeituras.append("NumeroLinha;MotivoErro;LinhaOriginal\n");
        conteudoErrosProcessos.append("NumeroLinha;MotivoErro;LinhaOriginal\n");

        try {
            System.out.println("Iniciando Passo 1: Lendo e validando " + chaveLeituras);

            GetObjectRequest getLeiturasRequest = GetObjectRequest.builder()
                    .bucket(bucketRaw).key(chaveLeituras).build();

            try (ResponseInputStream<GetObjectResponse> s3Stream = s3.getObject(getLeiturasRequest);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(s3Stream, StandardCharsets.UTF_8))) {

                String linha;
                int numeroLinha = 0;
                while ((linha = reader.readLine()) != null) {
                    numeroLinha++;
                    if (numeroLinha == 1) continue;

                    try {
                        Leitura leituraValidada = validarLeitura(linha, numeroLinha, banco);

                        String chave = leituraValidada.getMacAddress() + "_" + leituraValidada.getDatetime();
                        dadosMapeados.put(chave, new LeituraComProcessos(leituraValidada));

                    } catch (ValidacaoCsvException e) {
                        System.err.println("ERRO [leituras.csv] Linha " + numeroLinha + ": " + e.getMessage());
                        String linhaOriginal = linha.replace("\n", " ").replace("\r", " ");
                        conteudoErrosLeituras.append(numeroLinha).append(";")
                                .append(e.getMessage()).append(";")
                                .append(linhaOriginal).append("\n");
                    }
                }
            }
            System.out.println("Passo 1 concluído. " + dadosMapeados.size() + " leituras validadas.");


            System.out.println("Iniciando Passo 2: Lendo e validando " + chaveProcessos);

            GetObjectRequest getProcessosRequest = GetObjectRequest.builder()
                    .bucket(bucketRaw).key(chaveProcessos).build();

            try (ResponseInputStream<GetObjectResponse> s3Stream = s3.getObject(getProcessosRequest);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(s3Stream, StandardCharsets.UTF_8))) {

                String linha;
                int numeroLinha = 0;
                while ((linha = reader.readLine()) != null) {
                    numeroLinha++;
                    try {
                        Processos processoValidado = validarProcesso(linha, numeroLinha, dadosMapeados);

                        String chave = processoValidado.getMacAddress() + "_" + processoValidado.getTimestamp();
                        dadosMapeados.get(chave).adicionarProcesso(processoValidado);

                    } catch (NumberFormatException e) {
                        String linhaOriginal = linha.replace("\n", " ").replace("\r", " ");
                        conteudoErrosProcessos.append(numeroLinha).append(";")
                                .append("Dado não numérico").append(";")
                                .append(linhaOriginal).append("\n");
                    } catch (ValidacaoCsvException e) {
                        System.err.println("ERRO [processos.csv] Linha " + numeroLinha + ": " + e.getMessage());
                        String linhaOriginal = linha.replace("\n", " ").replace("\r", " ");
                        conteudoErrosProcessos.append(numeroLinha).append(";")
                                .append(e.getMessage()).append(";")
                                .append(linhaOriginal).append("\n");
                    }
                }
            }
            System.out.println("Passo 2 concluído. Processos mapeados.");

            System.out.println("Iniciando Passo 3: Gerando CSV de saída com Top 10...");

            conteudoSaidaFinal.append("datetime;mac_address;fk_empresa;os;cpu_maquina_percent;ram_maquina_percent;cpu_maquina_temp;")
                    .append("processo_id;processo_nome;processo_cpu_uso;processo_memoria_uso\n");

            for (LeituraComProcessos lcp : dadosMapeados.values()) {
                Leitura leitura = lcp.getLeitura();
                for (Processos p : lcp.getTop10ProcessosPorCpu()) {
                    conteudoSaidaFinal.append(leitura.getDatetime()).append(";")
                            .append(leitura.getMacAddress()).append(";")
                            .append(leitura.getFkEmpresa()).append(";")
                            .append(leitura.getOperationSystem()).append(";")
                            .append(leitura.getCpuPercent()).append(";")
                            .append(leitura.getRamPercent()).append(";")
                            .append(leitura.getCpuTemperature()).append(";")
                            .append(p.getId()).append(";")
                            .append(p.getNome()).append(";")
                            .append(p.getUsoCpu()).append(";")
                            .append(p.getUsoMemoria()).append("\n");
                }
            }
            System.out.println("Passo 3 concluído. CSV de saída gerado.");

            if (conteudoSaidaFinal.length() > 0) {
                System.out.println("Iniciando upload do arquivo enriquecido para: " + chaveSaidaTrusted);
                PutObjectRequest putRequest = PutObjectRequest.builder()
                        .bucket(bucketTrusted).key(chaveSaidaTrusted).contentType("text/csv").build();
                s3.putObject(putRequest, RequestBody.fromString(conteudoSaidaFinal.toString(), StandardCharsets.UTF_8));
                System.out.println("Upload do arquivo tratado concluído!");
            } else {
                System.out.println("Aviso: Nenhum dado foi processado para o arquivo final.");
            }

            if (conteudoErrosLeituras.length() > "NumeroLinha;MotivoErro;LinhaOriginal\n".length()) {
                System.out.println("Iniciando upload do relatório de erros (Leituras) para: " + chaveErrosLeituras);
                PutObjectRequest putErrosRequest = PutObjectRequest.builder()
                        .bucket(bucketTrusted).key(chaveErrosLeituras).contentType("text/csv").build();
                s3.putObject(putErrosRequest, RequestBody.fromString(conteudoErrosLeituras.toString(), StandardCharsets.UTF_8));
                System.out.println("Upload do relatório de erros (Leituras) concluído!");
            } else {
                System.out.println("Nenhum erro de validação encontrado em leituras.csv.");
            }

            if (conteudoErrosProcessos.length() > "NumeroLinha;MotivoErro;LinhaOriginal\n".length()) {
                System.out.println("Iniciando upload do relatório de erros (Processos) para: " + chaveErrosProcessos);
                PutObjectRequest putErrosRequest = PutObjectRequest.builder()
                        .bucket(bucketTrusted).key(chaveErrosProcessos).contentType("text/csv").build();
                s3.putObject(putErrosRequest, RequestBody.fromString(conteudoErrosProcessos.toString(), StandardCharsets.UTF_8));
                System.out.println("Upload do relatório de erros (Processos) concluído!");
            } else {
                System.out.println("Nenhum erro de validação encontrado em processos.csv.");
            }

        } catch (S3Exception e) {
            System.err.println("Erro do S3: " + e.awsErrorDetails().errorMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Erro de I/O: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Ocorreu um erro inesperado: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (s3 != null) {
                s3.close();
                System.out.println("Cliente S3 fechado.");
            }
            if (banco != null) {
                banco.close();
            }
        }
    }

    private static Leitura validarLeitura(String linha, int numeroLinha, BitwareSQL banco) throws ValidacaoCsvException {
        if (linha == null || linha.trim().isEmpty()) {
            throw new ValidacaoCsvException("Linha está totalmente em branco.");
        }

        String[] campos = linha.split(";", -1);

        if (campos.length != COLUNAS_LEITURA) {
            throw new ValidacaoCsvException(
                    "Número incorreto de colunas. Esperado: " + COLUNAS_LEITURA + ", Encontrado: " + campos.length);
        }

        for (int i = 0; i < campos.length; i++) {
            if (campos[i] == null || campos[i].trim().isEmpty()) {
                throw new ValidacaoCsvException("A coluna " + (i + 1) + " está nula ou em branco.");
            }
        }

        String macAddress = campos[8];
        int fkEmpresa = banco.CompararBanco(macAddress);
        if (fkEmpresa == 0) {
            throw new ValidacaoCsvException("MAC Address não encontrado no banco de dados: " + macAddress);
        }

        Leitura leitura = new Leitura(campos);
        leitura.setFkEmpresa(fkEmpresa);
        return leitura;
    }

    private static Processos validarProcesso(String linha, int numeroLinha, Map<String, LeituraComProcessos> mapaLeituras) throws ValidacaoCsvException {
        if (linha == null || linha.trim().isEmpty()) {
            throw new ValidacaoCsvException("Linha está totalmente em branco.");
        }

        String[] campos = linha.split(",", -1);

        if (campos.length != COLUNAS_PROCESSO) {
            throw new ValidacaoCsvException(
                    "Número incorreto de colunas. Esperado: " + COLUNAS_PROCESSO + ", Encontrado: " + campos.length);
        }

        for (int i = 0; i < campos.length; i++) {
            if (i == 2) continue;
            if (campos[i] == null || campos[i].trim().isEmpty()) {
                throw new ValidacaoCsvException("A coluna " + (i + 1) + " está nula ou em branco.");
            }
        }

        String macAddress = campos[5];
        String timestamp = campos[0];
        String chaveJoin = macAddress + "_" + timestamp;

        if (!mapaLeituras.containsKey(chaveJoin)) {
            throw new ValidacaoCsvException("Processo órfão (Leitura correspondente não existe ou falhou na validação)");
        }

        return new Processos(campos);
    }
}