package com.bitcoin.pi.etl.processor;

import com.bitcoin.pi.DB.BitwareSQL;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Processa linhas do Trusted, compara com parâmetros e cria chamados no DB.
 * Retorna Map<idEmpresa, List<String>> onde cada String é:
 * datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;motivo_chamado;id_empresa;mac_address
 */
public class AlertGenerator {

    private final BitwareSQL banco;

    public AlertGenerator(BitwareSQL banco) {
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
                int fkMaquina = banco.getFkMaquinaByMac(mac);
                StringBuilder motivo = new StringBuilder();

                // checar cada componente (busca usando fkEmpresa conforme especificado)
                checkAndMaybeCreate(fkMaquina, idEmpresa, "cpu_percent", cpu, mac, motivo);
                if (gpu != null) checkAndMaybeCreate(fkMaquina, idEmpresa, "gpu_percent", gpu, mac, motivo);
                checkAndMaybeCreate(fkMaquina, idEmpresa, "cpu_temperature", cpuTemp, mac, motivo);
                if (gpuTemp != null) checkAndMaybeCreate(fkMaquina, idEmpresa, "gpu_temperature", gpuTemp, mac, motivo);

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

    private void checkAndMaybeCreate(int fkMaquina, int idEmpresa, String componente, double valor, String mac, StringBuilder motivo) {
        // tenta pegar parametro por fkMaquina -> se null, busca ParametrosGeraisEmpresa por idEmpresa
        Integer param = banco.getParametroByEmpresa(idEmpresa, componente); // método usa fkEmpresa -> componente
        if (param == null) {
            // fallback para parametro por maquina (caso queira) - mas conforme você pediu, priorizamos fk_empresa
            param = banco.getParametro(fkMaquina, componente);
        }

        if (param == null) return; // sem parametro -> não avalia

        double upper = param * 1.05;
        double lower = param * 0.95;

        // regra que você ajustou: se uso maior que parametro => "bom" mas se temperatura alta => ruim; se abaixo => ruim
        boolean isTemperature = componente.toLowerCase().contains("temperature");

        if (valor > upper) {
            // para temperaturas, acionar alerta (ruim). para uso (cpu/gpu) é bom, mas você quer ainda registrar?
            String problema;
            String prioridade;
            if (isTemperature) {
                problema = String.format("Empresa[%d][%s] - %s acima do esperado (valor: %.2f, param: %d)", idEmpresa, mac, componente, valor, param);
                prioridade = "Alta";
                banco.criarChamado(fkMaquina, problema, prioridade, "Aberto", null);
            } else {
                // uso acima do parametro — você disse que é bom; só gerar aviso de atenção (opcional)
                problema = String.format("Empresa[%d][%s] - %s acima do parâmetro (valor: %.2f, param: %d) - Atenção", idEmpresa, mac, componente, valor, param);
                prioridade = "Baixa";
                banco.criarChamado(fkMaquina, problema, prioridade, "Aberto", null);
            }
            if (motivo.length() > 0) motivo.append(" | ");
            motivo.append(problema);
        } else if (valor < lower) {
            // abaixo do esperado -> ruim
            String problema = String.format("Empresa[%d][%s] - %s abaixo do esperado (valor: %.2f, param: %d)", idEmpresa, mac, componente, valor, param);
            String prioridade = isTemperature ? "Alta" : "Média";
            banco.criarChamado(fkMaquina, problema, prioridade, "Aberto", null);
            if (motivo.length() > 0) motivo.append(" | ");
            motivo.append(problema);
        }
    }
}
