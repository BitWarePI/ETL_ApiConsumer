package com.bitcoin.pi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Maquina {
    private Integer id;
    private String enderecoMac;
    private int fkEmpresa;

    // Novos atributos para as leituras de dados
    private String dataHoraLeitura;
    private Double usoGpu;
    private Double usoCpu;
    private Double tempGpu;
    private Double tempCpu;

    public Maquina() {
    }

    // Construtor completo (opcional, mas bom para ter)
    public Maquina(Integer id, String enderecoMac, Integer fkEmpresa,
                   String dataHoraLeitura, Double usoGpu, Double usoCpu,
                   Double tempGpu, Double tempCpu) {
        this.id = id;
        this.enderecoMac = enderecoMac;
        this.fkEmpresa = fkEmpresa;
        this.dataHoraLeitura = dataHoraLeitura;
        this.usoGpu = usoGpu;
        this.usoCpu = usoCpu;
        this.tempGpu = tempGpu;
        this.tempCpu = tempCpu;
    }

    public Maquina(String enderecoMac) {
        this.enderecoMac = enderecoMac;
    }

    // --- Getters e Setters existentes ---
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getEnderecoMac() {
        return enderecoMac;
    }

    public void setEnderecoMac(String enderecoMac) {
        this.enderecoMac = enderecoMac;
    }

    public int getFkEmpresa() {
        return fkEmpresa;
    }

    public void setFkEmpresa(int fkEmpresa) {
        this.fkEmpresa = fkEmpresa;
    }

    // --- Novos Getters e Setters ---
    public String getDataHoraLeitura() {
        return dataHoraLeitura;
    }

    public void setDataHoraLeitura(String dataHoraLeitura) {
        this.dataHoraLeitura = dataHoraLeitura;
    }

    public Double getUsoGpu() {
        return usoGpu;
    }

    public void setUsoGpu(Double usoGpu) {
        this.usoGpu = usoGpu;
    }

    public Double getUsoCpu() {
        return usoCpu;
    }

    public void setUsoCpu(Double usoCpu) {
        this.usoCpu = usoCpu;
    }

    public Double getTempGpu() {
        return tempGpu;
    }

    public void setTempGpu(Double tempGpu) {
        this.tempGpu = tempGpu;
    }

    public Double getTempCpu() {
        return tempCpu;
    }

    public void setTempCpu(Double tempCpu) {
        this.tempCpu = tempCpu;
    }

    @Override
    public String toString() {
        return "Maquina{" +
                "id=" + id +
                ", enderecoMac='" + enderecoMac + '\'' +
                ", fkEmpresa=" + fkEmpresa +
                ", dataHoraLeitura='" + dataHoraLeitura + '\'' +
                ", usoGpu=" + usoGpu +
                ", usoCpu=" + usoCpu +
                ", tempGpu=" + tempGpu +
                ", tempCpu=" + tempCpu +
                '}';
    }

    public void lerCsv() {
        String arquivoEntrada = "src/leituras.csv";
        String arquivoSaida = "src/maquinas_validadas.csv";
        String separador = ";";

        BitwareSQL banco = new BitwareSQL();

        List<Maquina> maquinasValidadas = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(arquivoEntrada), StandardCharsets.UTF_8))) {

            reader.readLine();

            String linha;
            while ((linha = reader.readLine()) != null) {
                String[] dados = linha.split(separador);

                if (dados.length < 9) continue;

                String dataHora = dados[0];
                String usoCpu = dados[2];
                String usoGpu = dados[3];
                String tempCpu = dados[6];
                String tempGpu = dados[7];
                String enderecoMac = dados[8];

                Maquina maquina = new Maquina(enderecoMac);
                maquina.setDataHoraLeitura(dataHora);
                maquina.setUsoCpu(Double.parseDouble(usoCpu.replace(",", ".")));
                maquina.setUsoGpu(Double.parseDouble(usoGpu.replace(",", ".")));
                maquina.setTempCpu(Double.parseDouble(tempCpu.replace(",", ".")));
                maquina.setTempGpu(Double.parseDouble(tempGpu.replace(",", ".")));


                int fkEmpresa = banco.CompararBanco(enderecoMac);

                if (fkEmpresa > 0) {
                    maquinasValidadas.add(maquina);
                    maquina.setFkEmpresa(fkEmpresa);
                    System.out.println("Máquina encontrada!");
                } else {
                    System.out.println("Máquina ignorada (não encontrada no banco)");
                }
            }


            escreverCsvValidado(arquivoSaida, maquinasValidadas, separador);
            System.out.println("Arquivo gerado com sucesso: " + arquivoSaida);

        } catch (Exception e) {
            System.out.println("Erro ao processar o arquivo: " + e.getMessage());
        }
    }

    private void escreverCsvValidado(String nomeArquivo, List<Maquina> maquinas, String separador) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(nomeArquivo))) {

            writer.write("datetime;uso_cpu;uso_gpu;temp_cpu;temp_gpu;mac_address;fk_empresa");
            writer.newLine();

            for (Maquina m : maquinas) {
                System.out.println("FK EMPRESA: " + m.getFkEmpresa());
                writer.write(m.getDataHoraLeitura() + separador +
                        m.getUsoCpu() + separador +
                        m.getUsoGpu() + separador +
                        m.getTempCpu() + separador +
                        m.getTempGpu() + separador +
                        m.getEnderecoMac() + separador +
                        m.getFkEmpresa());
                writer.newLine();

                System.out.println("Novo csv validado!");
            }

        } catch (IOException e) {
            System.out.println("Erro ao salvar o arquivo: " + e.getMessage());
        }
    }
}
