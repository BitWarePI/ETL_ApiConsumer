package com.bitcoin.pi.etl;

import com.bitcoin.pi.db.BitwareDatabase;
import com.sun.tools.jconsole.JConsoleContext;

import java.util.ArrayList;
import java.util.List;

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
}
