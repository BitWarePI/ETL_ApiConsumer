package com.bitcoin.pi.etl.transform;

import com.bitcoin.pi.model.Leitura;
import com.bitcoin.pi.model.LeituraComProcessos;
import com.bitcoin.pi.model.Processos;

import java.util.Map;

public class TransformadorDados {

    public String gerarCsvEnriquecido(Map<String, LeituraComProcessos> dados) {
        StringBuilder saida = new StringBuilder();
        saida.append("datetime;mac_address;fk_empresa;os;cpu_maquina_percent;ram_maquina_percent;cpu_maquina_temp;")
                .append("processo_id;processo_nome;processo_cpu_uso;processo_memoria_uso\n");

        for (LeituraComProcessos lcp : dados.values()) {
            Leitura leitura = lcp.getLeitura();
            for (Processos p : lcp.getTop10ProcessosPorCpu()) {
                saida.append(leitura.getDatetime()).append(";")
                        .append(leitura.getMacAddress()).append(";")
                        .append(leitura.getFkEmpresa()).append(";")
                        .append(leitura.getCpuPercent()).append(";")
                        .append(leitura.getCpuTemperature()).append(";")
                        .append(p.getId()).append(";")
                        .append(p.getNome()).append(";")
                        .append(p.getUsoCpu()).append(";")
                        .append(p.getUsoMemoria()).append("\n");
            }
        }
        return saida.toString();
    }

}
