package com.bitcoin.pi;

import com.bitcoin.pi.api.JiraClient;
import com.bitcoin.pi.db.BitwareDatabase;
import com.bitcoin.pi.etl.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDate;
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

                // montar LeiturasCLIENT.csv (cabecalho + linhas com motivo)
                String header = "datetime;cpu_percent;gpu_percent;cpu_temperature;gpu_temperature;motivo_chamado;id_empresa;mac_address\n";
                StringBuilder sbLeitClient = new StringBuilder();
                sbLeitClient.append(header);
                for (String l : linhas) sbLeitClient.append(l).append("\n");

                // enviar para client/{idEmpresa}/{dd-MM-yyyy}/
                carregador.uploadPorEmpresaEDia(idEmpresa, hoje, Map.of(
                        "LeiturasCLIENT.csv", sbLeitClient.toString()));

                // também enviar erros coletados (se existirem) para client
                if (!relErros.isEmpty() && !relErros.equals("NumeroLinha;MotivoErro;LinhaOriginal\n")) {
                    carregador.uploadPorEmpresaEDia(idEmpresa, hoje, Map.of(
                            "erros_leituras.csv", relErros
                    ));
                }



                StringBuilder sbProc = new StringBuilder();
                for (String l : linhasProcessos) sbProc.append(l).append("\n");

                carregador.uploadPorEmpresaEDia(idEmpresa, hoje, Map.of(
                        "processos.csv", sbProc.toString()
                ));
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
