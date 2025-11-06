package com.bitcoin.pi.etl.kpi;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Gera CSVs de KPI por empresa (últimas N horas).
 * Retorna Map<idEmpresa, csvString>
 */
public class KpiGenerator {

    private static final DateTimeFormatter fmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public Map<Integer, String> gerarKpisPorEmpresa(List<String> linhasTrusted, int horas) {
        Map<Integer, List<Record>> porEmpresa = new HashMap<>();
        Instant agora = Instant.now();
        Instant limite = agora.minus(Duration.ofHours(horas));
        int numeroLinha = 0;

        for (String l : linhasTrusted) {
            numeroLinha++;
            if (numeroLinha == 1) continue;
            if (l == null || l.trim().isEmpty()) continue;

            String[] cols = l.split(";", -1);
            if (cols.length != 7) continue;

            try {
                int idEmpresa = Integer.parseInt(cols[0].trim());
                String dtStr = cols[1].trim();
                // supomos ISO_LOCAL_DATE_TIME; se diferente, mude aqui
                LocalDateTime ldt = LocalDateTime.parse(dtStr, fmt);
                Instant inst = ldt.toInstant(ZoneOffset.UTC);

                if (inst.isBefore(limite)) continue;

                double cpu = Double.parseDouble(cols[2]);
                double gpu = cols[3].isEmpty() ? Double.NaN : Double.parseDouble(cols[3]);
                double cpuTemp = Double.parseDouble(cols[4]);
                double gpuTemp = cols[5].isEmpty() ? Double.NaN : Double.parseDouble(cols[5]);

                porEmpresa.computeIfAbsent(idEmpresa, k -> new ArrayList<>())
                        .add(new Record(inst, cpu, gpu, cpuTemp, gpuTemp));
            } catch (Exception ex) {
                // ignorar linha
            }
        }

        Map<Integer, String> result = new HashMap<>();
        for (Map.Entry<Integer, List<Record>> e : porEmpresa.entrySet()) {
            Integer idEmpresa = e.getKey();
            List<Record> rs = e.getValue();
            double avgCpu = rs.stream().mapToDouble(r -> r.cpu).average().orElse(0.0);
            double avgGpu = rs.stream().mapToDouble(r -> Double.isNaN(r.gpu)?0.0:r.gpu).average().orElse(0.0);
            double avgCpuTemp = rs.stream().mapToDouble(r -> r.cpuTemp).average().orElse(0.0);
            double avgGpuTemp = rs.stream().mapToDouble(r -> Double.isNaN(r.gpuTemp)?0.0:r.gpuTemp).average().orElse(0.0);

            String header = "data_inicio;data_final;media_porcentagem_CPU;media_porcentagem_GPU;media_temperatura_CPU;media_temperatura_GPU;id_empresa\n";
            String inicio = rs.get(0).timestamp.toString();
            String fim = Instant.now().toString();

            StringBuilder sb = new StringBuilder();
            sb.append(header);
            sb.append(inicio).append(";").append(fim).append(";")
                    .append(String.format("%.2f", avgCpu)).append(";")
                    .append(String.format("%.2f", avgGpu)).append(";")
                    .append(String.format("%.2f", avgCpuTemp)).append(";")
                    .append(String.format("%.2f", avgGpuTemp)).append(";")
                    .append(idEmpresa).append("\n");

            result.put(idEmpresa, sb.toString());
        }
        return result;
    }

    private static class Record {
        Instant timestamp;
        double cpu, gpu, cpuTemp, gpuTemp;
        Record(Instant t, double cpu, double gpu, double cpuTemp, double gpuTemp) {
            this.timestamp = t; this.cpu = cpu; this.gpu = gpu; this.cpuTemp = cpuTemp; this.gpuTemp = gpuTemp;
        }
    }
}
