package com.bitcoin.pi.etl;

import com.bitcoin.pi.db.BitwareDatabase;
import software.amazon.awssdk.services.s3.model.CSVOutput;

import java.util.*;
import java.util.stream.Collectors;


 // Processamento (Trusted): compara com parâmetros e cria chamados no DB.
 // Retorna Map<idEmpresa, List<String>> onde cada String é:
 // datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;motivo_chamado;id_empresa;mac_address
public class AlertGenerator {

    private BitwareDatabase banco;

    public AlertGenerator(BitwareDatabase banco) {
        this.banco = banco;
    }

    public Map<Integer, List<String>> processarTrustedEGerarChamados(List<String> linhasTrusted) {
        Map<Integer, List<String>> porEmpresa = new HashMap<>();
        int numeroLinha = 0;
        for (String linha : linhasTrusted) {
            numeroLinha++;
            if (numeroLinha == 1) continue; // pular header
            if (linha == null || linha.trim().isEmpty()) continue;

            String[] cols = linha.split(";", -1);
            if (cols.length != 7) continue;

            try {
                int idEmpresa = Integer.parseInt(cols[0].trim());
                String datetime = cols[1].trim();
                double cpu = Double.parseDouble(cols[2].trim());
                String gpuStr = cols[3].trim();
                Double gpu = gpuStr.isEmpty() ? null : Double.parseDouble(gpuStr);
                double cpuTemp = Double.parseDouble(cols[4].trim());
                String gpuTempStr = cols[5].trim();
                Double gpuTemp = gpuTempStr.isEmpty() ? null : Double.parseDouble(gpuTempStr);
                String mac = cols[6].trim();

                // recuperar fkMaquina pelo mac (por tabela Maquina)
                int fkMaquina = banco.getFkEmpresaPeloMac(mac);
                // Permite atualizar objeto strig imutavel "String",
                // permite modificar o conteúdo sem criar novos objeto  s
                StringBuilder motivo = new StringBuilder();

                // checar cada componente (busca usando fkEmpresa)
                verificarETalvezCriar(fkMaquina, idEmpresa, "cpu_percent", cpu, mac, motivo);
                if (gpu != null) verificarETalvezCriar(fkMaquina, idEmpresa, "gpu_percent", gpu, mac, motivo);
                verificarETalvezCriar(fkMaquina, idEmpresa, "cpu_temperature", cpuTemp, mac, motivo);
                if (gpuTemp != null) verificarETalvezCriar(fkMaquina, idEmpresa, "gpu_temperature", gpuTemp, mac, motivo);

                // montar linha de saída
                String motivoStr = motivo.length() == 0 ? "" : motivo.toString();
                String outLine = String.join(";", datetime, String.valueOf(cpu), (gpu==null?"":String.valueOf(gpu)),
                        String.valueOf(cpuTemp), (gpuTemp==null?"":String.valueOf(gpuTemp)),
                        motivoStr, String.valueOf(idEmpresa), mac);

                porEmpresa.computeIfAbsent(idEmpresa, k -> new ArrayList<>()).add(outLine);

            } catch (Exception ex) {
                // ignorar linha (ou logar)
                ex.printStackTrace();
            }
        }

        // opcional: ordenar por datetime por empresa
        porEmpresa.replaceAll((k, v) -> v.stream().sorted().collect(Collectors.toList()));
        return porEmpresa;
    }

    private void verificarETalvezCriar(int fkMaquina, int idEmpresa, String componente, double valor, String mac, StringBuilder motivo) {
        // tenta pegar parametro por fkMaquina -> se null, busca ParametrosGeraisEmpresa por idEmpresa
        Integer param = banco.getParametro(idEmpresa ,fkMaquina, componente);
        if (param == null) {
            param = banco.getParametroByEmpresa(idEmpresa, componente);
        }

        if (param == null) return; // sem parametro -> não avalia

        double upper = param * 1.05;
        double lower = param * 0.95;

        boolean isTemperature = componente.toLowerCase().contains("temperature");

        String nomeFormatado = formatarComponente(componente);

        if (valor > upper) {
            String problema;
            String prioridade;
            if (isTemperature) {
                problema = String.format("%s acima do esperado (valor: %.2f, parâmetro: %d)", nomeFormatado, valor, param);
                prioridade = "Alta";
                banco.criarChamado(fkMaquina, problema, prioridade, "Aberto", null);
            } else {
                problema = String.format("%s acima do parâmetro (valor: %.2f, parâmetro: %d) - Atenção", nomeFormatado, valor, param);
                prioridade = "Baixa";
                banco.criarChamado(fkMaquina, problema, prioridade, "Aberto", null);
            }
            if (motivo.length() > 0) motivo.append(" | ");
            motivo.append(problema);
        } else if (valor < lower) {
            String problema = String.format("%s abaixo do esperado (valor: %.2f, parâmetro: %d)", nomeFormatado, valor, param);
            String prioridade = isTemperature ? "Alta" : "Média";
            banco.criarChamado(fkMaquina, problema, prioridade, "Aberto", null);
            if (motivo.length() > 0) motivo.append(" | ");
            motivo.append(problema);
        }

    }

     public static Map<String, List<String>> pegarLeiturasPorMaquina(List<String> linhasTrusted) {

         Map<String, List<String>> porMac = new HashMap<>();
         int numeroLinha = 0;

         for (String linha : linhasTrusted) {
             numeroLinha++;
             if (numeroLinha == 1) continue; // pular header
             if (linha == null || linha.trim().isEmpty()) continue;

             String[] cols = linha.split(";", -1);
             if (cols.length != 7) continue;

             try {
                 String mac = cols[6].trim();

                 int idEmpresa = Integer.parseInt(cols[0].trim());
                 String datetime = cols[1].trim();
                 double cpu = Double.parseDouble(cols[2].trim());
                 String gpuStr = cols[3].trim();
                 Double gpu = gpuStr.isEmpty() ? null : Double.parseDouble(gpuStr);
                 double cpuTemp = Double.parseDouble(cols[4].trim());
                 String gpuTempStr = cols[5].trim();
                 Double gpuTemp = gpuTempStr.isEmpty() ? null : Double.parseDouble(gpuTempStr);


                 String outLine = String.join(";",
                         datetime,
                         String.valueOf(cpu),
                         (gpu == null ? "" : String.valueOf(gpu)),
                         String.valueOf(cpuTemp),
                         (gpuTemp == null ? "" : String.valueOf(gpuTemp)),
                         String.valueOf(idEmpresa),
                         mac
                 );

                 porMac.computeIfAbsent(mac, k -> new ArrayList<>()).add(outLine);

             } catch (Exception ex) {
                 ex.printStackTrace();
             }
         }

         porMac.replaceAll((k, v) -> v.stream().sorted().collect(Collectors.toList()));

         return porMac;
     }

     private String formatarComponente(String componente) {
         return switch (componente) {
             case "cpu_percent" -> "Uso de CPU (%)";
             case "gpu_percent" -> "Uso de GPU (%)";
             case "cpu_temperature" -> "Temperatura da CPU (°C)";
             case "gpu_temperature" -> "Temperatura da GPU (°C)";
             default -> componente;
         };
     }

     // Individual Isaak (Desafio técnico)
     public Map<Integer, String> gerarCsvChamadosPorEmpresa(BitwareDatabase banco) {

         Map<Integer, String> csvsPorEmpresa = new HashMap<>();

         // 1. Buscar IDs das empresas
         List<Integer> empresas = banco.listarIdsEmpresas();
         String header = "idChamado;fkMaquina;nomeMaquina;enderecoMac;prioridade;problema;status;dataAbertura\n";

         for (Integer idEmpresa : empresas) {

             List<Map<String, String>> ocorrencias = banco.buscarOcorrencias(idEmpresa);

             if (ocorrencias.isEmpty()) continue;

             StringBuilder sb = new StringBuilder();
             sb.append(header);

             for (Map<String, String> o : ocorrencias) {
                 sb.append(o.get("idChamado")).append(";")
                         .append(o.get("fkMaquina")).append(";")
                         .append(o.get("nomeMaquina")).append(";")
                         .append(o.get("enderecoMac")).append(";")
                         .append(o.get("prioridade")).append(";")
                         .append(o.get("problema")).append(";")
                         .append(o.get("status")).append(";")
                         .append(o.get("dataAbertura")).append("\n");
             }

             csvsPorEmpresa.put(idEmpresa, sb.toString());
         }

         return csvsPorEmpresa;
     }
 }
