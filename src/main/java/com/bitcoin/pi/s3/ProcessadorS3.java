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

        // Seleciona o bucket que você vai utilizar
        String nomeBucket = "amzn-bitware-v1";

        // Seleciona o caminho completo do arquivo de leitura
        String chaveArquivoLeitura = "dados/leituras.csv";

        // Seleciona o caminho completo do arquivo de escrita
        String chaveArquivoEscrita = "dados-tratados/relatorio_tratado.csv";

        // Selecione o caminho para o arquivo que vai guardar os erros
        String chaveArquivoErros = "dados-erros/relatorio_de_erros.csv";


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

            // StringBuilder para guardar as linhas com erro
            StringBuilder conteudoErros = new StringBuilder();
            // Adiciona um cabeçalho ao arquivo de erros para o Jira
            conteudoErros.append("NumeroLinha;MotivoErro;LinhaOriginal\n");


            // Contador de linha para sabermos o cabeçalho e logar erros
            int numeroLinha = 0;

            try (ResponseInputStream<GetObjectResponse> s3InputStream = s3.getObject(getRequest);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8))) {

                String linha;
                while ((linha = reader.readLine()) != null) {
                    numeroLinha++;

                    // Lógica para pular o cabeçalho (linha 1)
                    // Nós adicionamos o cabeçalho ao arquivo de saída e pulamos a validação
                    if (numeroLinha == 1) {
                        conteudoTratado.append(linha).append("\n");
                        continue; // Pula para a próxima iteração (próxima linha)
                    }

                    // --- 4. TRATAMENTO E VALIDAÇÃO ---
                    // ATUALIZADO: Bloco try-catch para validar cada linha
                    try {
                        // Se a linha for válida, ela é retornada
                        String linhaValidada = validarLinhaCsv(linha, numeroLinha);

                        // Adiciona a linha válida ao nosso arquivo de saída
                        conteudoTratado.append(linhaValidada).append("\n");

                    } catch (ValidacaoCsvException e) {
                        // Se a exceção for pega, a linha é inválida!
                        // Nós registramos o erro e pulamos a linha
                        // O processamento continua para as próximas linhas.
                        System.err.println("ERRO: Linha " + numeroLinha + " pulada. Motivo: " + e.getMessage());

                        // Guarda a informação do erro na StringBuilder de erros
                        // Remove quebras de linha da 'linha' original para não quebrar o CSV de erro
                        String linhaOriginalFormatada = linha.replace("\n", " ").replace("\r", " ");

                        conteudoErros.append(numeroLinha)
                                .append(";")
                                .append(e.getMessage()) // Motivo do erro
                                .append(";")
                                .append(linhaOriginalFormatada) // A linha exata que falhou
                                .append("\n");
                    }
                }
            }

            System.out.println("Leitura e tratamento concluídos. Total de linhas lidas: " + numeroLinha);

            // ATUALIZADO: Verifica se o 'conteudoTratado' tem mais do que apenas o cabeçalho
            if (numeroLinha <= 1) {
                System.out.println("Aviso: O arquivo lido estava vazio ou continha apenas o cabeçalho. Nenhum dado tratado foi salvo.");
            } else {
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

                System.out.println("Upload do arquivo tratado concluído com sucesso!");
            }

            // Bloco para salvar o arquivo de ERROS
            // Verifica se a StringBuilder de erros tem mais do que o cabeçalho
            if (conteudoErros.length() > "NumeroLinha;MotivoErro;LinhaOriginal\n".length()) {
                System.out.println("Iniciando upload do relatório de erros para: " + chaveArquivoErros);

                PutObjectRequest putErrosRequest = PutObjectRequest.builder()
                        .bucket(nomeBucket)
                        .key(chaveArquivoErros)
                        .contentType("text/csv")
                        .build();

                s3.putObject(putErrosRequest,
                        RequestBody.fromString(conteudoErros.toString(), StandardCharsets.UTF_8));

                System.out.println("Upload do relatório de erros concluído!");
            } else {
                System.out.println("Nenhum erro de validação encontrado. Arquivo de erros não foi gerado.");
            }


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