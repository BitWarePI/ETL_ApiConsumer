//package com.bitcoin.pi.etl;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class Teste {
//    public static void main(String[] args) {
//        List<String>trusted = List.of("""
//2;2025-05-08 00:00:00;76;78;59.0;50.1;a1:b2:c3:d4:e5:f6
//2;2025-05-08 00:00:00;78;37;68.1;63.6;ff:ee:dd:cc:bb:aa
//2;2025-05-09 00:00:00;47;41;60.8;69.7;e8:5c:5f:1e:b4:1d
//2;2025-05-09 00:00:00;47;45;55.7;56.9;a1:b2:c3:d4:e5:f6
//2;2025-05-10 00:00:00;78;39;55.9;64.3;ff:ee:dd:cc:bb:aa
//2;2025-05-11 03:00:00;47;68;68.9;52.9;e8:5c:5f:1e:b4:1d
//2;2025-05-11 00:00:00;95;64;60.6;54.1;a1:b2:c3:d4:e5:f6
//2;2025-05-12 00:00:00;85;60;62.0;63.0;ff:ee:dd:cc:bb:aa
//2;2025-05-13 00:00:00;71;42;73.2;55.2;e8:5c:5f:1e:b4:1d
//2;2025-05-14 00:00:00;44;67;55.9;58.8;a1:b2:c3:d4:e5:f6
//2;2025-05-14 00:00:00;76;84;67.8;67.3;ff:ee:dd:cc:bb:aa
//2;2025-05-15 00:00:00;63;41;61.8;52.0;e8:5c:5f:1e:b4:1d
//2;2025-05-15 00:00:00;86;38;67.3;64.0;a1:b2:c3:d4:e5:f6
//2;2025-05-16 00:00:00;69;54;73.3;56.2;ff:ee:dd:cc:bb:aa
//2;2025-05-16 00:00:00;58;41;59.5;69.5;e8:5c:5f:1e:b4:1d
//2;2025-05-17 00:00:00;46;46;62.5;56.5;a1:b2:c3:d4:e5:f6
//2;2025-05-18 00:00:00;79;67;60.5;59.6;ff:ee:dd:cc:bb:aa
//2;2025-05-18 00:00:00;44;49;68.6;54.0;e8:5c:5f:1e:b4:1d
//2;2025-05-19 00:00:00;41;79;61.7;60.0;a1:b2:c3:d4:e5:f6
//2;2025-05-20 00:00:00;72;47;62.0;65.8;ff:ee:dd:cc:bb:aa
//2;2025-05-21 00:00:00;43;67;63.7;54.0;e8:5c:5f:1e:b4:1d
//2;2025-05-21 00:00:00;52;89;55.8;67.7;a1:b2:c3:d4:e5:f6
//2;2025-05-22 00:00:00;76;42;73.7;68.3;ff:ee:dd:cc:bb:aa
//2;2025-05-22 00:00:00;60;52;62.2;66.7;e8:5c:5f:1e:b4:1d
//2;2025-05-23 00:00:00;76;39;66.2;67.9;a1:b2:c3:d4:e5:f6
//2;2025-05-23 00:00:00;69;49;74.2;51.5;ff:ee:dd:cc:bb:aa
//2;2025-05-24 00:00:00;66;73;61.1;68.7;e8:5c:5f:1e:b4:1d
//2;2025-05-25 00:00:00;69;49;57.4;52.8;a1:b2:c3:d4:e5:f6
//                """.split("\n"));
//
//        Map<String, List<String>> leiturasPorMaquina = AlertGenerator.pegarLeiturasPorMaquina(trusted);
//
//        Map<String, List<String[]>> valorPorData = new HashMap<>();
//        //map usa chave e valor(key e values), sempre definido nessa ordem, ex: "idmac": ["","","",""]
//        for (List<String> maquina : leiturasPorMaquina.values()) {
//
//            //valor é cada um dos registros por maquina, de cada valor das 3 em 3 horas ele executa essa funcao
//            for(String valor : maquina){
//                String[] valores = valor.split(";");
//
//                String data = valores[0].split(" ")[0];
//                //separando por data
//                List<String[]> dataExiste = valorPorData.get(data); //datetime é o 0
//                if(dataExiste == null){
//                    dataExiste = new ArrayList<>();
//                }
//                dataExiste.add(valores);
//                valorPorData.put(data, dataExiste);
//            }
//        }
//
//        List<String>file = new ArrayList<>();
//        for(List<String[]> dia : valorPorData.values()){
//            double cpu = 0;
//            double gpu = 0;
//            double cpuTemp = 0;
//            double gpuTemp = 0;
//
//            for(String[] dados : dia){
//                cpu += Double.parseDouble(dados[2]);
//                gpu += Double.parseDouble(dados[3]);
//                cpuTemp += Double.parseDouble(dados[4]);
//                gpuTemp += Double.parseDouble(dados[5]);
//            }
//            cpu = cpu / dia.size();
//            gpu = gpu / dia.size();
//            cpuTemp += cpuTemp / dia.size();
//            gpuTemp += gpuTemp / dia.size();
//            String linha = String.join(";",
//                    String.valueOf(cpu),
//                    String.valueOf(gpu),
//                    String.valueOf(cpuTemp),
//                    String.valueOf(gpuTemp)
//            );
//            file.add(linha);
//        }
//
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
//            writer.write(String.valueOf(file));}
//        catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//}
