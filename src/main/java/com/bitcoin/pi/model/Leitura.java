package com.bitcoin.pi.model;

public class Leitura {
    private Integer idEmpresa;
    private String datetime;
    private Double cpuPercent;
    private Double gpuPercent;
    private Double cpuTemperature;
    private Double gpuTemperature;
    private String macAddress;

    public Leitura(String[] campos) {
        // construtor a partir de array padronizado: id_empresa;datetime;cpu;gpu;cpuTemp;gpuTemp;mac
        this.idEmpresa = Integer.parseInt(campos[0]);
        this.datetime = campos[1];
        this.cpuPercent = Double.parseDouble(campos[2]);
        this.gpuPercent = campos[3].isEmpty() ? null : Double.parseDouble(campos[3]);
        this.cpuTemperature = Double.parseDouble(campos[4]);
        this.gpuTemperature = campos[5].isEmpty() ? null : Double.parseDouble(campos[5]);
        this.macAddress = campos[6];
    }

    // getters
    public Integer getFkEmpresa() { return idEmpresa; }
    public String getDatetime() { return datetime; }
    public Double getCpuPercent() { return cpuPercent; }
    public Double getGpuPercent() { return gpuPercent; }
    public Double getCpuTemperature() { return cpuTemperature; }
    public Double getGpuTemperature() { return gpuTemperature; }
    public String getMacAddress() { return macAddress; }
}
