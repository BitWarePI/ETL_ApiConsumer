package com.bitcoin.pi.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ProcessadorS3 {

    private static final int COLUNAS_ESPERADAS = 9;

    public static void main(String[] args) {

        Region regiao = Region.US_EAST_1;

        // Selecione o bucket que você vai utilizar
        String nomeBucket = "amzn-bitware-v1";

        // O selecione o caminho completo do arquivo que vamos LER
        String chaveArquivoLeitura = "dados/leituras.csv";

        // O nome/caminho completo do arquivo que você quer ESCREVER
        String chaveArquivoEscrita = "dados-tratados/relatorio_tratado.csv";


        // 2. Iniciando o Bucket
        // O .build() procurar automaticamente suas credenciais
        // (no arquivo ~/.aws/credentials ou nas variáveis de ambiente)
        S3Client s3 = S3Client.builder()
                .region(regiao)
                .build();

        System.out.println("Cliente S3 iniciado. Conectando ao bucket: " + nomeBucket);

        try {
            // --- 3. LEITURA (DOWNLOAD) ---
            System.out.println("Iniciando leitura do arquivo: " + chaveArquivoLeitura);

            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(nomeBucket)
                    .key(chaveArquivoLeitura)
                    .build();

            StringBuilder conteudoTratado = new StringBuilder();

            // NOVO: Contador de linha para sabermos o cabeçalho e logar erros
            int numeroLinha = 0;

            // ATUALIZADO: try-with-resources com charset UTF-8
            try (ResponseInputStream<GetObjectResponse> s3InputStream = s3.getObject(getRequest);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8))) {

                String linha;
                while ((linha = reader.readLine()) != null) {
                    numeroLinha++;

                    // NOVO: Lógica para pular o cabeçalho (linha 1)
                    // Nós adicionamos o cabeçalho ao arquivo de saída e pulamos a validação
                    if (numeroLinha == 1) {
                        conteudoTratado.append(linha).append("\n");
                        continue; // Pula para a próxima iteração (próxima linha)
                    }

                    // --- 4. TRATAMENTO E VALIDAÇÃO ---
                    // ATUALIZADO: Bloco try-catch para validar cada linha
                    try {
                        // O método validarLinhaCsv vai fazer o trabalho sujo
                        // Se a linha for válida, ela é retornada
                        String linhaValidada = validarLinhaCsv(linha, numeroLinha);

                        // Adiciona a linha válida ao nosso arquivo de saída
                        conteudoTratado.append(linhaValidada).append("\n");

                    } catch (ValidacaoCsvException e) {
                        // Se a exceção for pega, a linha é inválida!
                        // Nós registramos o erro e pulamos a linha
                        // O processamento continua para as próximas linhas.
                        System.err.println("ERRO: Linha " + numeroLinha + " pulada. Motivo: " + e.getMessage());
                    }
                }
            }

            System.out.println("Leitura e tratamento concluídos. Total de linhas lidas: " + numeroLinha);

            // ATUALIZADO: Verifica se o 'conteudoTratado' tem mais do que apenas o cabeçalho
            if (numeroLinha <= 1) {
                System.out.println("Aviso: O arquivo lido estava vazio ou continha apenas o cabeçalho.");
                return;
            }

            // --- 5. ESCRITA (UPLOAD) ---
            System.out.println("Iniciando upload do arquivo tratado para: " + chaveArquivoEscrita);

            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(nomeBucket)
                    .key(chaveArquivoEscrita)
                    .contentType("text/csv") // Boa prática
                    .build();

            // Converte o StringBuilder para bytes e envia
            s3.putObject(putRequest,
                    RequestBody.fromString(conteudoTratado.toString(), StandardCharsets.UTF_8));

            System.out.println("Upload concluído com sucesso!");

        } catch (S3Exception e) {
            // Trata erros específicos da AWS (Acesso Negado, Arquivo Não Encontrado)
            System.err.println("Erro do S3: " + e.awsErrorDetails().errorMessage());
            e.printStackTrace();
        } catch (IOException e) {
            // Trata erros de leitura/escrita do stream
            System.err.println("Erro de I/O: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. FECHA O CLIENTE
            // Importante para liberar recursos
            if (s3 != null) {
                s3.close();
                System.out.println("Cliente S3 fechado.");
            }
        }
    }
    private static String validarLinhaCsv(String linha, int numeroLinha) throws ValidacaoCsvException {

        // 1. Verifica se a linha inteira está em branco (null, "" e " ")
        if (linha == null || linha.trim().isEmpty()) {
            throw new ValidacaoCsvException("Linha está totalmente em branco.");
        }

        // 2. Divide a linha pelos campos
        // O -1 no split() garante que campos vazios no final (ex: "a;b;;") sejam contados
        String[] campos = linha.split(";", -1);

        // 3. Valida o número de colunas
        if (campos.length != COLUNAS_ESPERADAS) {
            throw new ValidacaoCsvException(
                    "Número incorreto de colunas. Esperado: " + COLUNAS_ESPERADAS +
                            ", Encontrado: " + campos.length);
        }

        // 4. Itera e valida cada campo individualmente
        for (int i = 0; i < campos.length; i++) {
            String campo = campos[i];

            // A verificação trim().isEmpty() cobre os 3 casos: null, "" (vazio) e " " (branco)
            if (campo == null || campo.trim().isEmpty()) {
                throw new ValidacaoCsvException(
                        "A coluna " + (i + 1) + " (índice " + i + ") está nula, vazia ou em branco."
                );
            }
        }

        // Retorna a linha original para ser salva no novo arquivo.
        return linha;
    }
}