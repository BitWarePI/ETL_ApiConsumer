package com.bitcoin.pi;

import com.bitcoin.pi.api.JiraClient;
import com.bitcoin.pi.db.BitwareDatabase;
import com.bitcoin.pi.etl.*;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ProcessadorS3 {

    public static void main(String[] args) {
        Region regiao = Region.US_EAST_1;
        String bucketRaw = "s3-raw-bitwarepi";
        String bucketTrusted = "s3-trusted-bitwarepi";
        String bucketClient = "s3-client-bitwarepi";

        S3Client s3 = S3Client.builder().region(regiao).build();
        BitwareDatabase banco = new BitwareDatabase();
      
        System.out.println("Cliente S3 iniciado.");

        String pathLeiturasRaw = "dados/leituras.csv";
        String pathProcessosRaw = "dados/processos.csv";
        String pathHardwareRaw = "dados/hardware.csv";
        String pathLeiturasTrusted = "dados/LeiturasTRUSTED.csv";
        String pathErrosLeituras = "dados-erros/erros_leituras.csv";
        String pathChamados = "dados/chamados.csv";

        try {
            ExtratorS3 extrator = new ExtratorS3(s3, bucketRaw);
            ValidadorLeituras validador = new ValidadorLeituras();

            System.out.println("Baixando leituras do RAW...");
            List<String> linhasRaw = extrator.baixarArquivo(pathLeiturasRaw);

            System.out.println("Baixando processos do RAW...");
            List<String> linhasProcessos = extrator.baixarArquivo(pathProcessosRaw);

            System.out.println("Baixando hardware do RAW...");
            List<String> linhasHardware = extrator.baixarArquivo(pathHardwareRaw);

            System.out.println("Tratando leituras (validação)...");
            List<String> linhasValidas = validador.tratarLinhasRaw(linhasRaw);
            String relErros = validador.getRelatorioErros();

            // grava erros - aqui salvamos no trusted erros para envio posterior
            TrustedWriter trustedWriter = new TrustedWriter(s3, bucketTrusted);
            if (!relErros.isEmpty() && !relErros.equals("NumeroLinha;MotivoErro;LinhaOriginal\n")) {
                trustedWriter.escreverConteudo(pathErrosLeituras, relErros);
                System.out.println("Erros escritos em: " + pathErrosLeituras);
            } else {
                System.out.println("Nenhum erro de leitura detectado.");
            }

            // grava LeiturasTRUSTED.csv no bucket trusted
            trustedWriter.escreverTrusted(pathLeiturasTrusted, linhasValidas);
            System.out.println("Arquivo Trusted escrito: " + pathLeiturasTrusted);

            // Processamento: lendo do Trusted
            System.out.println("Lendo do Trusted para gerar chamados e arquivos por empresa...");
            List<String> linhasTrusted = extrator.baixarArquivoFromBucket(bucketTrusted, pathLeiturasTrusted);

            // gerar motivo de chamado para cada linha e acumular por empresa
            AlertGenerator alertGenerator = new AlertGenerator(banco);

            // gerar LeiturasCLIENT.csv por empresa
            // aqui vamos criar map de conteúdo por empresa
            Map<Integer, List<String>> leiturasPorEmpresa = alertGenerator.processarTrustedEGerarChamados(linhasTrusted);

            // Upload client por empresa(id)/data
            CarregadorS3 carregador = new CarregadorS3(s3, bucketClient);
            LocalDate hoje = LocalDate.now();

            // INDIVIDUAL AMORIM
            //*********************************************************************************************
            System.out.println("Iniciando processamento de Hardware...");
            Map<Integer, List<String>> hardwarePorEmpresaMap = new HashMap<>();

            if (linhasHardware != null && !linhasHardware.isEmpty()) {
                // i=1 para pular o cabeçalho original
                for (int i = 1; i < linhasHardware.size(); i++) {
                    String linha = linhasHardware.get(i);
                    String[] dados = linha.split(";");

                    // Estrutura: datetime;macAddress;so... (macAddress é index 1)
                    if (dados.length > 1) {
                        String mac = dados[1];

                        // Usa a função existente na sua classe BitwareDatabase
                        int fkEmpresa = banco.getFkEmpresaPeloMac(mac);

                        if (fkEmpresa > 0) {
                            // Se a chave ainda não existe no mapa, cria a lista
                            hardwarePorEmpresaMap.computeIfAbsent(fkEmpresa, k -> new ArrayList<>()).add(linha);
                        } else {
                            System.out.println("Aviso: Empresa não encontrada para MAC: " + mac);
                        }
                    }
                }
            }
            String headerHardware = "datetime;macAddress;so;qtdRam;cpuCor;qtdGpu;qtdDisco\n";

            for (Map.Entry<Integer, List<String>> entry : hardwarePorEmpresaMap.entrySet()) {
                Integer idEmpresa = entry.getKey();
                List<String> linhasDaEmpresa = entry.getValue();

                StringBuilder sbHard = new StringBuilder();
                sbHard.append(headerHardware);
                for (String l : linhasDaEmpresa) {
                    sbHard.append(l).append("\n");
                }

                // Caminho final será: bucket-client/{idEmpresa}/hardware.csv
                carregador.uploadPorHardware(idEmpresa, "hardware.csv", sbHard.toString());
                System.out.println("Hardware enviado para empresa ID " + idEmpresa);
            }
            //*********************************************************************************************


            for (Map.Entry<Integer, List<String>> entry : leiturasPorEmpresa.entrySet()) {
                Integer idEmpresa = entry.getKey();
                List<String> linhas = entry.getValue();

                Map<LocalDate, StringBuilder> arquivosPorData = new HashMap<>();
                String header = "datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;motivo_chamado;id_empresa;mac_address\n";

                for (String l : linhas) {
                    String dataStr = l.split(";", -1)[0].split(" ")[0];
                    LocalDate dataLinha = LocalDate.parse(dataStr);

                    arquivosPorData
                            .computeIfAbsent(dataLinha, d -> new StringBuilder(header))
                            .append(l)
                            .append("\n");
                }

                // Envia UM arquivo por data
                for (Map.Entry<LocalDate, StringBuilder> arq : arquivosPorData.entrySet()) {
                    LocalDate data = arq.getKey();
                    String conteudo = arq.getValue().toString();

                    carregador.uploadPorEmpresaEDia(idEmpresa, data, Map.of(
                            "LeiturasCLIENT.csv", conteudo
                    ));
                }

                if (!relErros.isEmpty() && !relErros.equals("NumeroLinha;MotivoErro;LinhaOriginal\n")) {

                    Map<LocalDate, StringBuilder> errosPorData = new HashMap<>();

                    for (String errLine : relErros.split("\n")) {
                        if (errLine.startsWith("NumeroLinha") || errLine.isBlank()) continue;

                        String[] parts = errLine.split(";", -1);
                        if (parts.length < 3) continue;

                        // A linha original está na 3ª coluna → pegar a data dela
                        String linhaOriginal = parts[2];
                        String dataStr = linhaOriginal.split(";", -1)[0].split(" ")[0];
                        LocalDate dataErro = LocalDate.parse(dataStr);

                        errosPorData
                                .computeIfAbsent(dataErro, d -> new StringBuilder("NumeroLinha;MotivoErro;LinhaOriginal\n"))
                                .append(errLine).append("\n");
                    }

                    for (Map.Entry<LocalDate, StringBuilder> err : errosPorData.entrySet()) {
                        carregador.uploadPorEmpresaEDia(idEmpresa, err.getKey(), Map.of(
                                "erros_leituras.csv", err.getValue().toString()
                        ));
                    }
                }

                Map<LocalDate, StringBuilder> processosPorData = new HashMap<>();

                for (String l : linhasProcessos) {
                    String dataStr = l.split(";", -1)[0].split(" ")[0];
                    LocalDate dataProc = LocalDate.parse(dataStr);

                    processosPorData
                            .computeIfAbsent(dataProc, d -> new StringBuilder())
                            .append(l).append("\n");
                }

                for (Map.Entry<LocalDate, StringBuilder> pr : processosPorData.entrySet()) {
                    carregador.uploadPorEmpresaEDia(idEmpresa, pr.getKey(), Map.of(
                            "processos.csv", pr.getValue().toString()
                    ));
                }
            }

            System.out.println("Processamento concluído.");

            // Inciando conecxão no jira
            JiraClient jiraClient = new JiraClient(
                    "https://bitwarepi-1760010438510.atlassian.net/rest/api/3/issue",
                    "bitwarepi@gmail.com",
                    "ATATT3xFfGF0F1yyy3bpBGaxxjrH9DFg7vmgu677GAgbv2YGPdoM_9G0KEUREi" +
                            "dX0NCQuYl0NI1Xz8Q3rUL0FWmLqk5Cr8yi6GXno5mmZF3ernt_RjkQ" +
                            "h7r6z8WuIzQas35wWoUsNElEiZFSXdRA6G9158VFGo_9ymgyr8yRTr" +
                            "rVFbcELvUnph4=4141905E"
            );

            System.out.println("Buscando chamados pendentes para enviar ao Jira...");
            List<Map<String, String>> chamados = banco.listarChamadosNaoSincronizados();


            // INDIVIDUAL ISAAK (DESAFIO TÉCNICO)
            //*********************************************************************************************
            try{
                AlertGenerator alertas = new AlertGenerator(banco);
                System.out.println("Gerando CSV de chamados por empresa...");
                Map<Integer, String> csvChamados = alertas.gerarCsvChamadosPorEmpresa(banco);

                for (Map.Entry<Integer, String> entry : csvChamados.entrySet()) {
                    int idEmpresa = entry.getKey();
                    String conteudo = entry.getValue();

                    carregador.uploadChamados(idEmpresa, "chamados.csv", conteudo.toString());

                    System.out.println("Chamados enviados para empresa: " + idEmpresa);
                }
            } catch (Exception e){
                System.out.println(e);
            }
            //*********************************************************************************************

            // INDIVIDUAL NICOLAS JAVED
            //*********************************************************************************************
            Map<String, List<String>> leiturasPorMaquina = alertGenerator.pegarLeiturasPorMaquina(linhasTrusted);

            for (String mac : leiturasPorMaquina.keySet()) {

                List<String> linhas = leiturasPorMaquina.get(mac);

                if (linhas.isEmpty()) continue;

                String[] colsPrimeiraLinha = linhas.get(0).split(";");
                int idEmpresa = Integer.parseInt(colsPrimeiraLinha[5]);

                StringBuilder sb = new StringBuilder();
                sb.append("datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;id_empresa;mac_address\n");

                for (String linha : linhas) {
                    sb.append(linha).append("\n");
                }

                String conteudoCsv = sb.toString();
                String nomeArquivo = mac + ".csv";

                carregador.uploadPorMaquina(idEmpresa, nomeArquivo, conteudoCsv);

                System.out.println("Arquivo gerado e enviado: " + nomeArquivo);
            }
            //*****************************************************************************

            // INDIVIDUAL GABRIELA
            //*********************************************************************************************

            // LEITURAS POR MÁQUINA
            Map<String, List<String[]>> porEmpresa = new HashMap<>();

            //esse for faz a ordenação por empresa!!!¹¹¹
            for (String linha : linhasTrusted) {
                String[] partes = linha.split(";");
                String idEmpresa = partes[0];
                porEmpresa
                        .computeIfAbsent(idEmpresa, x -> new ArrayList<>()).add(partes);
            }

            for (Map.Entry<String, List<String[]>> entry : porEmpresa.entrySet()){
                Map<String, List<String[]>> valorPorData = new HashMap<>();
                for (String[] valores : entry.getValue()) {
                    String data = valores[1].split(" ")[0]; // coluna 1 é datetime
                    valorPorData.computeIfAbsent(data, x -> new ArrayList<>()).add(valores); //se n tiver o registro ele cria um
                }

                StringBuilder csv = new StringBuilder();
                csv.append("data;cpu;gpu;cpuTemp;gpuTemp\n");

                //ordena pela data e passa por um foreach, stream e uma funcao usada para arrays, maps e semelhantes
                valorPorData.entrySet().stream()
                        .sorted(Comparator.comparing(e -> LocalDate.parse(e.getKey())))
                        .forEach(entryData -> {
                            String data = entryData.getKey();
                            List<String[]> dia = entryData.getValue();

                            double cpu = 0;
                            double gpu = 0;
                            double cpuTemp = 0;
                            double gpuTemp = 0;

                            for (String[] dados : dia) {
                                cpu += Double.parseDouble(dados[2]);
                                gpu += Double.parseDouble(dados[3]);
                                cpuTemp += Double.parseDouble(dados[4]);
                                gpuTemp += Double.parseDouble(dados[5]);
                            }

                            int total = dia.size();

                            cpu /= total;
                            gpu /= total;
                            cpuTemp /= total;
                            gpuTemp /= total;

                            LocalDate d = LocalDate.parse(data); // data = "2025-05-08"
                            String dataBR = d.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));

                            csv.append(dataBR).append(";")
                                    .append(String.format("%.2f",cpu)).append(";")
                                    .append(String.format("%.2f",gpu)).append(";")
                                    .append(String.format("%.2f",cpuTemp)).append(";")
                                    .append(String.format("%.2f",gpuTemp)).append("\n");
                        });

                // ENVIA PARA O S3
                S3Uploader.enviarCsvParaS3(
                        bucketClient,
                        String.format("%s/medias/medias.csv", entry.getKey()),
                        csv.toString()
                );
            }
            //*********************************************************************************************

            for (Map<String, String> ch : chamados) {
                String problema = ch.get("problema");
                String prioridade = ch.get("prioridade");
                int idChamado = Integer.parseInt(ch.get("id"));

                if (jiraClient.criarCard(problema, prioridade)) {
                    banco.marcarChamadoComoSincronizado(idChamado);
                    System.out.println("Card criado no Jira para chamado ID " + idChamado);
                } else {
                    System.out.println("Falha ao criar card no Jira para chamado ID " + idChamado);
                }
            }
            System.out.println("Sincronização dos chamados no banco com o Jira, concluida!");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (s3 != null) s3.close();
            if (banco != null) banco.close();
        }
    }
}
