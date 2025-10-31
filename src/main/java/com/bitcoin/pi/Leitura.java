package com.bitcoin.pi;

public class Leitura {

    String datetime;
    String operationSystem;
    double cpuPercent;
    double gpuPercent;
    double ramPercent;
    double diskPercent;
    double cpuTemperature;
    double gpuTemperature;
    String macAddress;

    private int fkEmpresa;

    public Leitura(String[] campos) {
        if (campos.length != 9) {
            throw new IllegalArgumentException("Linha do CSV de leituras não tem 9 colunas.");
        }
        this.datetime = campos[0];
        this.operationSystem = campos[1];
        this.cpuPercent = parseDouble(campos[2]);
        this.gpuPercent = parseDouble(campos[3]);
        this.ramPercent = parseDouble(campos[4]);
        this.diskPercent = parseDouble(campos[5]);
        this.cpuTemperature = parseDouble(campos[6]);
        this.gpuTemperature = parseDouble(campos[7]);
        this.macAddress = campos[8];
    }

    private double parseDouble(String valor) {
        if (valor == null || valor.trim().isEmpty()) return 0.0;
        return Double.parseDouble(valor.replace(",", "."));
    }

    public String getDatetime() { return datetime; }
    public String getOperationSystem() { return operationSystem; }
    public double getCpuPercent() { return cpuPercent; }
    public double getRamPercent() { return ramPercent; }
    public double getCpuTemperature() { return cpuTemperature; }
    public String getMacAddress() { return macAddress; }

    public int getFkEmpresa() { return fkEmpresa; }
    public void setFkEmpresa(int fkEmpresa) { this.fkEmpresa = fkEmpresa; }
}