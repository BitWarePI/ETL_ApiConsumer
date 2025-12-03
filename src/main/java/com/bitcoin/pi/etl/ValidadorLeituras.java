package com.bitcoin.pi.etl;

import com.bitcoin.pi.db.BitwareDatabase;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ValidadorLeituras {

    private BitwareDatabase bd = new BitwareDatabase();
    private String HEADER = "datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;mac_address";
    private StringBuilder erros = new StringBuilder("NumeroLinha;MotivoErro;LinhaOriginal\n");

    public List<String> tratarLinhasRaw(List<String> linhasRaw) {
        List<String> linhasValidas = new ArrayList<>();
        int numeroLinha = 0;

        // Valida o Header
        for (String linha : linhasRaw) {
            numeroLinha++;
            if (numeroLinha == 1) {
                if (!linha.trim().equalsIgnoreCase(HEADER)) {
                    erros.append(numeroLinha)
                            .append(";Header inválido (esperado: ").append(HEADER).append(");")
                            .append(linha).append("\n");
                }
                continue;
            }

            if (linha == null || linha.trim().isEmpty()) {
                erros.append(numeroLinha).append(";Linha em branco;").append(linha).append("\n");
                continue;
            }

            String[] cols = linha.split(";", -1);
            if (cols.length != 6) {
                erros.append(numeroLinha).append(";Colunas incorretas (esperado 7);").append(linha).append("\n");
                continue;
            }

            try {
                String datetime = cols[0].trim();
                String cpu = cols[1].trim().replace(",", ".");
                String gpu = cols[2].trim().replace(",", ".");
                String cpuTemp = cols[3].trim().replace(",", ".");
                String gpuTemp = cols[4].trim().replace(",", ".");
                String mac = cols[5].trim();

                if (datetime.isEmpty() || cpu.isEmpty() || mac.isEmpty()) {
                    erros.append(numeroLinha).append(";Campo obrigatório vazio;").append(linha).append("\n");
                    continue;
                }

                String idEmpresa = bd.getIdEmpresaPorMacAddress(mac).toString();

                // parse numéricos (se falhar -> erro)
                Double cpuVal = Double.parseDouble(cpu);
                Double.parseDouble(cpuTemp);
                // gpu e gpuTemp podem estar vazios (mas já convertidos para string)
                if (!gpu.isEmpty()) Double.parseDouble(gpu);
                if (!gpuTemp.isEmpty()) Double.parseDouble(gpuTemp);

                // Regra: cpu = 0 => tratar como inválido
                if (cpuVal == 0.0) {
                    erros.append(numeroLinha).append(";cpu_percent=0;").append(linha).append("\n");
                    continue;
                }

                // se chegou aqui => linha válida
                String normalized = String.join(";", idEmpresa, datetime, cpu, gpu, cpuTemp, gpuTemp, mac);
                linhasValidas.add(normalized);

            } catch (NumberFormatException ex) {
                erros.append(numeroLinha).append(";Dado numérico inválido;").append(linha).append("\n");
            } catch (Exception e) {
                erros.append(numeroLinha).append(";Erro inesperado:").append(e.getMessage()).append(";").append(linha).append("\n");
            }
        }
        return linhasValidas;
    }

    public String getRelatorioErros() {
        return erros.toString();
    }

    public static class Teste {
        public static void main(String[] args) {
            List<String> trusted = List.of("""
    2;2025-05-08 00:00:00;76;78;59.0;50.1;a1:b2:c3:d4:e5:f6
    2;2025-05-08 00:00:00;78;37;68.1;63.6;ff:ee:dd:cc:bb:aa
    2;2025-05-09 00:00:00;47;41;60.8;69.7;e8:5c:5f:1e:b4:1d
    2;2025-05-09 00:00:00;47;45;55.7;56.9;a1:b2:c3:d4;e5:f6
    2;2025-05-10 00:00:00;78;39;55.9;64.3;ff:ee:dd:cc:bb:aa
    2;2025-05-11 03:00:00;47;68;68.9;52.9;e8:5c:5f:1e:b4:1d
    2;2025-05-11 00:00:00;95;64;60.6;54.1;a1:b2:c3:d4:e5:f6
    2;2025-05-12 00:00:00;85;60;62.0;63.0;ff:ee:dd:cc:bb:aa
    2;2025-05-13 00:00:00;71;42;73.2;55.2;e8:5c:5f:1e:b4:1d
    2;2025-05-14 00:00:00;44;67;55.9;58.8;a1:b2:c3:d4:e5:f6
    2;2025-05-14 00:00:00;76;84;67.8;67.3;ff:ee:dd:cc:bb:aa
    2;2025-05-15 00:00:00;63;41;61.8;52.0;e8:5c:5f:1e:b4:1d
    2;2025-05-15 00:00:00;86;38;67.3;64.0;a1:b2:c3:d4:e5:f6
    2;2025-05-16 00:00:00;69;54;73.3;56.2;ff:ee:dd:cc:bb:aa
    2;2025-05-16 00:00:00;58;41;59.5;69.5;e8:5c:5f:1e:b4:1d
    2;2025-05-17 00:00:00;46;46;62.5;56.5;a1:b2:c3:d4:e5:f6
    2;2025-05-18 00:00:00;79;67;60.5;59.6;ff:ee:dd:cc:bb:aa
    2;2025-05-18 00:00:00;44;49;68.6;54.0;e8:5c:5f:1e:b4:1d
    2;2025-05-19 00:00:00;41;79;61.7;60.0;a1:b2:c3:d4:e5:f6
    2;2025-05-20 00:00:00;72;47;62.0;65.8;ff:ee:dd:cc:bb:aa
    2;2025-05-21 00:00:00;43;67;63.7;54.0;e8:5c:5f:1e:b4:1d
    2;2025-05-21 00:00:00;52;89;55.8;67.7;a1:b2:c3:d4:e5:f6
    2;2025-05-22 00:00:00;76;42;73.7;68.3;ff:ee:dd:cc:bb:aa
    2;2025-05-22 00:00:00;60;52;62.2;66.7;e8:5c:5f:1e:b4:1d
    2;2025-05-23 00:00:00;76;39;66.2;67.9;a1:b2:c3:d4:e5:f6
    2;2025-05-23 00:00:00;69;49;74.2;51.5;ff:ee:dd:cc:bb:aa
    2;2025-05-24 00:00:00;66;73;61.1;68.7;e8:5c:5f:1e:b4:1d
    2;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f6
    """.split("\n"));

            // LEITURAS POR MÁQUINA
            Map<String, List<String>> leiturasPorMaquina = AlertGenerator.pegarLeiturasPorMaquina(trusted);

            // AGRUPA POR DATA
            Map<String, List<String[]>> valorPorData = new HashMap<>();

            for (List<String> maquina : leiturasPorMaquina.values()) {
                for (String valor : maquina) {
                    String[] valores = valor.split(";");

                    String data = valores[0].split(" ")[0]; // coluna 1 é datetime
                    valorPorData.computeIfAbsent(data, x -> new ArrayList<>()).add(valores);
                }
            }

            // CALCULA MÉDIAS E MONTA CSV
            StringBuilder csv = new StringBuilder();
            csv.append("data;cpu;gpu;cpuTemp;gpuTemp\n");

            for (Map.Entry<String, List<String[]>> entry : valorPorData.entrySet()) {

                String data = entry.getKey();
                List<String[]> dia = entry.getValue();

                double cpu = 0;
                double gpu = 0;
                double cpuTemp = 0;
                double gpuTemp = 0;

                for (String[] dados : dia) {
                    cpu += Double.parseDouble(dados[1]);
                    gpu += Double.parseDouble(dados[2]);
                    cpuTemp += Double.parseDouble(dados[3]);
                    gpuTemp += Double.parseDouble(dados[4]);
                }

                int total = dia.size();
                System.out.println(total + " " + cpu + " " + gpu + " " + gpuTemp  + " "+ cpuTemp + " " + data);

                cpu /= total;
                gpu /= total;
                cpuTemp /= total;
                gpuTemp /= total;

                csv.append(data).append(";")
                        .append(String.format("%.2f",cpu)).append(";")
                        .append(String.format("%.2f",gpu)).append(";")
                        .append(String.format("%.2f",cpuTemp)).append(";")
                        .append(String.format("%.2f",gpuTemp)).append("\n");
            }

            // ENVIA PARA O S3
            S3Uploader.enviarCsvParaS3(
                    "bucket-client-2111",
                    "medias/medias.csv",
                    csv.toString()
            );
        }

        public static class S3Uploader {

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
    }
}
