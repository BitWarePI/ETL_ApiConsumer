package com.bitcoin.pi.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class LeituraComProcessos {
    private final Leitura leitura;
    private final List<Processos> processos = new ArrayList<>();

    public LeituraComProcessos(Leitura leitura) {
        this.leitura = leitura;
    }

    public Leitura getLeitura() { return leitura; }

    public void adicionarProcesso(Processos p) { processos.add(p); }

    public List<Processos> getTop10ProcessosPorCpu() {
        return processos.stream()
                .filter(p -> p.getUsoCpu() != null)
                .sorted(Comparator.comparingDouble(Processos::getUsoCpu).reversed())
                .limit(10)
                .collect(Collectors.toList());
    }
}
