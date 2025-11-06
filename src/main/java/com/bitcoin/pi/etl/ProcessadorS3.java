package com.bitcoin.pi.etl;

import com.bitcoin.pi.ApiClient.API_Rest;
import com.bitcoin.pi.DB.BitwareSQL;
import com.bitcoin.pi.etl.charger.CarregadorS3;
import com.bitcoin.pi.etl.extrator.ExtratorS3;
import com.bitcoin.pi.etl.kpi.KpiGenerator;
import com.bitcoin.pi.etl.processor.AlertGenerator;
import com.bitcoin.pi.etl.validation.ValidadorLeituras;
import com.bitcoin.pi.etl.writer.TrustedWriter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class ProcessadorS3 {

    public static void main(String[] args) {
        // inicia API externa (se necessário)
        API_Rest.ApiConsumer();

        Region regiao = Region.US_EAST_1;
        String bucketRaw = "s3-raw-bitwarepi";
        String bucketTrusted = "s3-trusted-bitwarepi";
        String bucketClient = "s3-client-bitwarepi";

        S3Client s3 = S3Client.builder().region(regiao).build();
        BitwareSQL banco = new BitwareSQL();

        // caminhos/chaves
        String chaveLeiturasRaw = "dados/leituras.csv";
        String chaveLeiturasTrusted = "dados/LeiturasTRUSTED.csv";
        String chaveErrosLeituras = "dados-erros/erros_leituras.csv";

        try {
            ExtratorS3 extrator = new ExtratorS3(s3, bucketRaw);
            ValidadorLeituras validador = new ValidadorLeituras();

            System.out.println("Baixando leituras do RAW...");
            List<String> linhasRaw = extrator.baixarArquivo(chaveLeiturasRaw);

            System.out.println("Tratando leituras (validação)...");
            List<String> linhasValidas = validador.tratarLinhasRaw(linhasRaw);
            String relErros = validador.getRelatorioErros();

            // grava erros (no raw ou em outro local) - aqui salvamos no trusted erros para envio posterior
            TrustedWriter trustedWriter = new TrustedWriter(s3, bucketTrusted);
            if (!relErros.isEmpty() && !relErros.equals("NumeroLinha;MotivoErro;LinhaOriginal\n")) {
                trustedWriter.escreverConteudo(chaveErrosLeituras, relErros);
                System.out.println("Erros escritos em: " + chaveErrosLeituras);
            } else {
                System.out.println("Nenhum erro de leitura detectado.");
            }

            // grava LeiturasTRUSTED.csv no bucket trusted
            trustedWriter.escreverTrusted(chaveLeiturasTrusted, linhasValidas);
            System.out.println("Arquivo Trusted escrito: " + chaveLeiturasTrusted);

            // Agora continuar o processamento lendo do Trusted
            System.out.println("Lendo do Trusted para gerar chamados e arquivos por empresa...");
            List<String> linhasTrusted = extrator.baixarArquivoFromBucket(bucketTrusted, chaveLeiturasTrusted);

            // gerar motivo de chamado para cada linha e acumular por empresa
            AlertGenerator alertGenerator = new AlertGenerator(banco);
            KpiGenerator kpiGenerator = new KpiGenerator();

            // Map<idEmpresa, List<String>> leiturasComMotivo = ... (vamos gerar os arquivos por empresa)
            Map<Integer, String> kpisPorEmpresaCsv = kpiGenerator.gerarKpisPorEmpresa(linhasTrusted, 24);

            // gerar LeiturasCLIENT.csv por empresa
            // aqui vamos criar map de conteúdo por empresa
            Map<Integer, List<String>> leiturasPorEmpresa = alertGenerator.processarTrustedEGerarChamados(linhasTrusted);

            // Upload para client por empresa/data
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

                // KPI
                String kpiCsv = kpisPorEmpresaCsv.getOrDefault(idEmpresa, "");

                // enviar para client/{idEmpresa}/{dd-MM-yyyy}/
                carregador.uploadPorEmpresaEDia(idEmpresa, hoje, Map.of(
                        "LeiturasCLIENT.csv", sbLeitClient.toString(),
                        "KPI.csv", kpiCsv
                ));
            }

            // também enviar erros coletados (se existirem) para client (opcional)
            if (!relErros.isEmpty() && !relErros.equals("NumeroLinha;MotivoErro;LinhaOriginal\n")) {
                // enviar para pasta de log global; aqui optamos por enviar para root dados-erros/ com data
                carregador.uploadGlobal("dados-erros/erros_leituras.csv", relErros);
            }

            System.out.println("Processamento concluído.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (s3 != null) s3.close();
            if (banco != null) banco.close();
        }
    }
}
