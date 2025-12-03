package com.bitcoin.pi.etl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class Teste {
    public static void main(String[] args) {
        List<String>trusted = List.of("""
2;2025-05-08 00:00:00;76;78;59.0;50.1;a1:b2:c3:d4:e5:f6
2;2025-05-08 00:00:00;78;37;68.1;63.6;ff:ee:dd:cc:bb:aa
2;2025-05-09 00:00:00;47;41;60.8;69.7;e8:5c:5f:1e:b4:1d
2;2025-05-09 00:00:00;47;45;55.7;56.9;a1:b2:c3:d4:e5:f6
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
3;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f4
3;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f5
3;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f7
3;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f7
4;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f7
4;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f7
4;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f7
4;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f7
4;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f7
                """.split("\n"));

        Map<String, List<String>> leiturasPorMaquina = AlertGenerator.pegarLeiturasPorMaquina(trusted);

        // LEITURAS POR MÁQUINA
        Map<String, List<String[]>> porEmpresa = new HashMap<>();

        //esse for faz a ordenação por empresa!!!¹¹¹
        for (String linha : trusted) {
            String[] partes = linha.split(";");
            String idEmpresa = partes[0];
            porEmpresa
                    .computeIfAbsent(idEmpresa, x -> new ArrayList<>()).add(partes);
        }

        String headerTrusted = "id_empresa;datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;mac_address\n";

        for (Map.Entry<String, List<String[]>> entry : porEmpresa.entrySet()){
            List<String[]> registrosDaEmpresa = entry.getValue();

            List<String[]> registrosOrdenados = registrosDaEmpresa.stream()
                    .sorted(Comparator.comparing(valores -> {
                        try {
                            return LocalDateTime.parse(valores[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        } catch (Exception e) {
                            return LocalDateTime.MIN;
                        }
                    }))
                    .collect(Collectors.toList());

            StringBuilder csvOrdenado = new StringBuilder();
            csvOrdenado.append(headerTrusted);

            for (String[] valores : registrosOrdenados) {
                String linha = String.join(";", valores);
                csvOrdenado.append(linha).append("\n");
            }

            try {
                S3Uploader.enviarCsvParaS3(
                        "bucket-client-2111",
                        String.format("%s/leiturasFormatadas/leituras.csv", entry.getKey()),
                        csvOrdenado.toString()
                );
                System.out.println("Arquivo ordenado e enviado");
            } catch (NumberFormatException e) {
                System.err.println("NÃO ENVIOU :(((( ");
            }
        }

        for (Map.Entry<String, List<String[]>> entry : porEmpresa.entrySet()){
            Map<String, List<String[]>> valorPorData = new HashMap<>();
            for (String[] valores : entry.getValue()) {
                String data = valores[1].split(" ")[0]; // coluna 1 é datetime
                valorPorData.computeIfAbsent(data, x -> new ArrayList<>()).add(valores); //se n tiver o registro ele cria um
            }

            StringBuilder csv = new StringBuilder();
            csv.append("data;cpu;gpu;cpuTemp;gpuTemp\n");

            //ordena pela data e passa por um foreach, stream e uma funcao usada para arrays, maps e semelhantes
            valorPorData.entrySet().stream()
                    .sorted(Comparator.comparing(e -> LocalDate.parse(e.getKey())))
                    .forEach(entryData -> {
                        String data = entryData.getKey();
                        List<String[]> dia = entryData.getValue();

                        double cpu = 0;
                        double gpu = 0;
                        double cpuTemp = 0;
                        double gpuTemp = 0;

                        for (String[] dados : dia) {
                            cpu += Double.parseDouble(dados[2]);
                            gpu += Double.parseDouble(dados[3]);
                            cpuTemp += Double.parseDouble(dados[4]);
                            gpuTemp += Double.parseDouble(dados[5]);
                        }

                        int total = dia.size();

                        cpu /= total;
                        gpu /= total;
                        cpuTemp /= total;
                        gpuTemp /= total;

                        LocalDate d = LocalDate.parse(data); // data = "2025-05-08"
                        String dataBR = d.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));

                        csv.append(dataBR).append(";")
                                .append(String.format("%.2f",cpu)).append(";")
                                .append(String.format("%.2f",gpu)).append(";")
                                .append(String.format("%.2f",cpuTemp)).append(";")
                                .append(String.format("%.2f",gpuTemp)).append("\n");
                    });

            // ENVIA PARA O S3
            S3Uploader.enviarCsvParaS3(
                    "bucket-client-2111",
                    String.format("%s/medias/medias.csv", entry.getKey()),
                    csv.toString()
            );


        }}}