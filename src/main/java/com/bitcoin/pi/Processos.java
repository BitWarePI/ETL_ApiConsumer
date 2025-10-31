package com.bitcoin.pi;

public class Processos {
    String timestamp;
    int id;
    String nome;
    double usoCpu;
    double usoMemoria;
    String macAddress;

    public Processos(String[] campos) {
        this.timestamp = campos[0];
        this.id = Integer.parseInt(campos[1]);
        this.nome = campos[2];
        this.usoCpu = Double.parseDouble(campos[3].replace(",", "."));
        this.usoMemoria = Double.parseDouble(campos[4].replace(",", "."));
        this.macAddress = campos[5];
    }

    public String getTimestamp() { return timestamp; }
    public int getId() { return id; }
    public String getNome() { return nome; }
    public double getUsoCpu() { return usoCpu; }
    public double getUsoMemoria() { return usoMemoria; }
    public String getMacAddress() { return macAddress; }
}