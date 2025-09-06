package com.sptech.school;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        double temp;
        String estado;
        while (true){
            temp = Math.round(Math.random()*60+40);
            if (temp > 90) {
                estado = "Situação crítica, encerre o minerador imediatamente.";
            } else if (temp > 70){
                estado = "Situação elevada, inspeção na máquina altamente recomendada.";
            } else {
                estado = "Situação normal, máquina dentro das especificações desejadas.";
            }
            System.out.println(" ");
            System.out.println("Temperatura atual: " + temp + "ºC");
            System.out.println(estado);
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}