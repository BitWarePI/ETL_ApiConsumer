package com.bitcoin.pi.etl.validation;

import com.bitcoin.pi.exceptions.ValidacaoCsv;
import com.bitcoin.pi.model.LeituraComProcessos;
import com.bitcoin.pi.model.Processos;

import java.util.List;
import java.util.Map;

public class ValidadorProcessos {

    private static final int COLUNAS_PROCESSO = 6;
    private final StringBuilder erros = new StringBuilder("NumeroLinha;MotivoErro;LinhaOriginal\n");

    public void validarProcessos(List<String> linhas, Map<String, LeituraComProcessos> leituras) {
        int numeroLinha = 0;
        for (String linha : linhas) {
            numeroLinha++;
            if (numeroLinha == 1) continue;

            try {
                String[] campos = linha.split(",", -1);
                if (campos.length != COLUNAS_PROCESSO)
                    throw new ValidacaoCsv("Número incorreto de colunas");

                String mac = campos[5];
                String timestamp = campos[0];
                String chave = mac + "_" + timestamp;

                if (!leituras.containsKey(chave))
                    throw new ValidacaoCsv("Processo órfão - sem leitura correspondente.");

                Processos proc = new Processos(campos);
                leituras.get(chave).adicionarProcesso(proc);

            } catch (ValidacaoCsv e) {
                erros.append(numeroLinha).append(";").append(e.getMessage()).append(";").append(linha).append("\n");
            }
        }
    }

    public String getRelatorioErros() {
        return erros.toString();
    }

}
