package com.bitcoin.pi;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class Maquina {
    private Integer id;
    private String enderecoMac;
    private int fkEmpresa;

    public Maquina() {}

    public Maquina(Integer id, String enderecoMac, Integer fkEmpresa) {
        this.id = id;
        this.enderecoMac = enderecoMac;
        this.fkEmpresa = fkEmpresa;
    }

    public Integer getId() {
        return id;
    }

    public String getEnderecoMac() {
        return enderecoMac;
    }

    public int getFkEmpresa() {
        return fkEmpresa;
    }

    public void setEnderecoMac(String enderecoMac) {
        this.enderecoMac = enderecoMac;
    }

    public void lerCsv() {
        String nomeArquivo = "src/leituras.csv";
        //parte para quebrar as colunas do csv pra pegar so o endereco mac
        String separador = ";";
        //ramdom para adicionar o id nas maquinas que podem ou nao bater com oq ta no banco
        Random random = new Random();

        BitwareSQL banco = new BitwareSQL();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(nomeArquivo), StandardCharsets.UTF_8 ))) {
            String linha;
            System.out.println("Conteúdo do arquivo: " + nomeArquivo);
            while ((linha = reader.readLine()) != null){
                String[] dados = linha.split(separador);

                String enderecoMac = dados[8];
                int idMaquina = random.nextInt(10) + 1; // id entre 1 e 10
                int fkEmpresa = 1;

                Maquina maquina = new Maquina(idMaquina, enderecoMac, fkEmpresa);
                System.out.printf("Máquina com o id: %d | Endereço MAC: %s | id da empresa: %d%n",
                        maquina.getId(), maquina.getEnderecoMac(), maquina.getFkEmpresa());

                banco.CompararBanco(maquina.getId(), maquina.getEnderecoMac(), maquina.getFkEmpresa());
            }
        }catch (IOException e){
            System.out.println("Erro ao ler o arquivo: " + nomeArquivo);
        }
    }
}

