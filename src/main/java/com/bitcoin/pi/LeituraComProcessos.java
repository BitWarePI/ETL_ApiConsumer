package com.bitcoin.pi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LeituraComProcessos {

    private Leitura leitura;
    private List<Processos> processos;

    public LeituraComProcessos(Leitura leitura) {
        this.leitura = leitura;
        this.processos = new ArrayList<>();
    }

    public void adicionarProcesso(Processos p) {
        this.processos.add(p);
    }

    public Leitura getLeitura() {
        return leitura;
    }

    public List<Processos> getTop10ProcessosPorCpu() {
        Collections.sort(processos, (p1, p2) -> Double.compare(p2.getUsoCpu(), p1.getUsoCpu()));

        return processos.subList(0, Math.min(processos.size(), 10));
    }
}