package com.bitcoin.pi;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Scanner;

public class MySql {
    String url;
    String user;
    String password;
    Connection connection;
    Statement statement;

    public MySql(String nameBD, String user, String password) {
//        this.url = "jdbc:mysql://44.210.193.181:3306/" + nameBD;
        this.url = "jdbc:mysql://localhost:3306/" + nameBD;
        this.user = user;
        this.password = password;
    }

    protected void conectBD(){
        try {
            connection = DriverManager.getConnection(url, user, password);
            System.out.println(">>> Banco conectado!");
        } catch (SQLException e) {
            System.out.println("====== Erro ao conectar no banco: " + e.getMessage() + " =======");
        }
    }

    protected void insertTable(Scanner sc){
        try {
            System.out.println("nome (max 100 caracter): ");
            String nome = sc.next().trim();
            System.out.println("genero (max 100 caracter): ");
            String genero = sc.next().trim();
            System.out.println("studio (max 100 caracter): ");
            String studio = sc.next().trim();
            System.out.println("qtd_episodio: ");
            int qtd_episodio = sc.nextInt();
            System.out.println("qtd_filme: ");
            int qtd_filme = sc.nextInt();
            System.out.println("eh_original (true ou false): ");
            boolean eh_original = sc.nextBoolean();
            System.out.println("ano_lancamento: ");
            int ano_lancamento = sc.nextInt();

            statement.execute(String.format(
                    "insert into anime (nome, genero, studio, qtd_episodio, qtd_filme, eh_original, ano_lancamento)" +
                            "values ('%s', '%s', '%s', %d, %d, %b, %d)",
                    nome, genero, studio, qtd_episodio, qtd_filme, eh_original, ano_lancamento)
            );
            System.out.println(">>> Registro inserido com sucesso!");
        } catch (Exception e) {
            System.out.println("===== Erro ao inserir registro: " + e.getMessage() + " =====");
        }
    }

    protected void consultTable(String table){
        try {
            ResultSet resultSet = statement.executeQuery("select * from "+ table +" ");
            System.out.println(">>> Registros selecionados com sucesso: ");

            while (resultSet.next()){
                System.out.printf("id: %d - nome: %s - genero: %s - studio: %s - qtd_episodio: %d - qtd_filme: %d - " +
                                "eh_original: %s - ano_lancamento: %d",
                        resultSet.getInt("id"),
                        resultSet.getString("nome"),
                        resultSet.getString("genero"),
                        resultSet.getString("studio"),
                        resultSet.getInt("qtd_episodio"),
                        resultSet.getInt("qtd_filme"),
                        resultSet.getString("eh_original"),
                        resultSet.getInt("ano_lancamento")
                );
                System.out.println("");
            }
        } catch (Exception e){
            System.out.println("====== Registros da tabela não encontrado! " + e.getMessage() +" =======");
        }
    }

    protected String[] search(){
        try {
            Scanner sc = new Scanner(System.in);
            System.out.println("Deseja alterar qual anime (nome): ");
            String name = sc.next().trim();

            // Validando
            ResultSet rs = statement.executeQuery("select * from anime where nome = '"+ name +"' ");
            boolean existe = rs.next();
            if (!existe){
                System.out.println("Anime não encontrado!");
                return null;
            }

            System.out.println("""
                Deseja alterar qual dado do anime:
                1 - Nome
                2 - genero
                3 - studio
                4 - qtd_episodio
                5 - qtd_filme
                6 - eh_original
                7 - ano_lancamento
                """);

            String atributo = switch (sc.nextInt()) {
                case 1 -> "nome";
                case 2 -> "genero";
                case 3 -> "studio";
                case 4 -> "qtd_episodio";
                case 5 -> "qtd_filme";
                case 6 -> "eh_original";
                case 7 -> "ano_lancamento";
                default -> null;
            };

            if (atributo == null){
                System.out.println("Opção invalida!");
                return null;
            }

            System.out.println("Insira o novo valor para '"+ atributo +"'");
            String valor = sc.next().trim();

            if (valor.isEmpty()){
                System.out.println("Valor invalido!");
                return null;
            }

            return new String[]{name, atributo, valor};
        } catch (SQLException e) {
            System.out.println("==== Erro nos dados fornecidos: " + e.getMessage() + " ======");
            return null;
        }
    }

    protected void exportCSV(){
        String outputFile = "src/animes.csv";

        try (ResultSet rs = statement.executeQuery("select * from anime");
             BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));
        )
        {
            ResultSetMetaData metaData = rs.getMetaData();
            int colCount = metaData.getColumnCount();

            // Cabeçalho
            for (int i = 1; i <= colCount; i++) {
                writer.append(metaData.getColumnName(i));
                if (i < colCount) writer.append(",");
            }
            writer.append("\n");

            // Dados
            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    writer.append(rs.getString(i));
                    if (i < colCount) writer.append(",");
                }
                writer.append("\n");
            }

            System.out.println(">>> Dados exportados com sucesso!");
        } catch (Exception e) {
            throw new RuntimeException("======= Erro ao exportar CSV: " + e.getMessage() + " ======");
        }
    }

    protected void closeBD(){
        try {
            connection.close();
            System.out.println(">>> Banco fechado com sucesso!");
        } catch (SQLException e) {
            System.out.println("======= Erro ao fecher banco "+ e.getMessage() +" =======");
        }
    }
}
