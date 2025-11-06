package com.bitcoin.pi.model;

public class Processos {
    private String timestamp;
    private String id;
    private String nome;
    private Double usoCpu;
    private Double usoMemoria;
    private String macAddress;

    public Processos(String[] cols) {
        // caso precise construir a partir de csv de processos (ajuste se formato diferente)
        this.timestamp = cols[0];
        this.id = cols[1];
        this.nome = cols[2];
        this.usoCpu = cols[3].isEmpty() ? null : Double.parseDouble(cols[3].replace(",", "."));
        this.usoMemoria = cols[4].isEmpty() ? null : Double.parseDouble(cols[4].replace(",", "."));
        this.macAddress = cols[5];
    }

    // getters
    public String getTimestamp() { return timestamp; }
    public String getId() { return id; }
    public String getNome() { return nome; }
    public Double getUsoCpu() { return usoCpu; }
    public Double getUsoMemoria() { return usoMemoria; }
    public String getMacAddress() { return macAddress; }
}
