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
import java.util.List;
import java.util.Map;

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
                    "https://bitwarepi-1760010438510.atlassian.net",
                    "bitwarepi@gmail.com",
                    "ATATT3xFfGF0bZcRrel-kCKKusKH4uywBSqEZFKPthnrXLeTpKd" +
                            "hips8op9g3Bh_DBjV7f-ISuhE38MX9yExH6Hg_5tIWSEAxrm3O9" +
                            "gZZm36g2ZBdScIFfMrhqZnCwmkq8a1kxjWWxsCcvmWYOH5E-3fvz" +
                            "3BjpH5502wl-M_3FoJlBYJhDkJ2fw=BAED5BC2"
            );

            System.out.println("Buscando chamados pendentes para enviar ao Jira...");
            List<Map<String, String>> chamados = banco.listarChamadosNaoSincronizados();

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
