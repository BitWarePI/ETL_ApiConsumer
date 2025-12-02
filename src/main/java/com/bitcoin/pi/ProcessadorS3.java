package com.bitcoin.pi;

import com.bitcoin.pi.api.JiraClient;
import com.bitcoin.pi.db.BitwareDatabase;
import com.bitcoin.pi.etl.CarregadorS3;
import com.bitcoin.pi.etl.ExtratorS3;
import com.bitcoin.pi.etl.AlertGenerator;
import com.bitcoin.pi.etl.ValidadorLeituras;
import com.bitcoin.pi.etl.TrustedWriter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessadorS3 {

    public static void main(String[] args) {
        Region regiao = Region.US_EAST_1;
        String bucketRaw = "s3-raw-bitwarepi777";
        String bucketTrusted = "s3-trusted-bitwarepi777";
        String bucketClient = "s3-client-bitwarepi777";

        S3Client s3 = S3Client.builder().region(regiao).build();
        BitwareDatabase banco = new BitwareDatabase();
      
        System.out.println("Cliente S3 iniciado.");

        String pathLeiturasRaw = "dados/leituras.csv";
        String pathProcessosRaw = "dados/processos.csv";
        String pathLeiturasTrusted = "dados/LeiturasTRUSTED.csv";
        String pathErrosLeituras = "dados-erros/erros_leituras.csv";

        try {
            ExtratorS3 extrator = new ExtratorS3(s3, bucketRaw);
            ValidadorLeituras validador = new ValidadorLeituras();

            System.out.println("Baixando leituras do RAW...");
            List<String> linhasRaw = extrator.baixarArquivo(pathLeiturasRaw);

            System.out.println("Baixando processos do RAW...");
            List<String> linhasProcessos = extrator.baixarArquivo(pathProcessosRaw);

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
                    "ATATT3xFfGF0K0xx81slFgC5DdDiv8uV4RRE2eMne1Rv-4lFqi2RnPFJBJl1u48v" +
                            "kVJLVv2oyYD5vJcjcdtuGOQiILPm84CP_j1hic-sjxA9aPiRJY_tmM_aVZ48Js5" +
                            "vBumc1OcqW4QsLjWdz21rnaF6B8jnxyFyCtCLpffhB3imquDbs1PcCm8=35FCF9C1"
            );

            System.out.println("Buscando chamados pendentes para enviar ao Jira...");
            List<Map<String, String>> chamados = banco.listarChamadosNaoSincronizados();

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
