package com.bitcoin.pi;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class BitwareSQL {
    private Connection conn;

    public BitwareSQL() {
        try {
            String url = "jdbc:mysql://localhost:3306/bitware_db";

            // MUDAR AQUIIII NA HORA DE TESTARRR!!!!!!!!!!!!!
//            String user = "aluno";
//            String password = "123456";
            String user = "bitware";
            String password = "sptech";

            this.conn = DriverManager.getConnection(url, user, password);
            System.out.println("Conectado ao MySQL com sucesso!!!!!!!!!!!!!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int CompararBanco(String enderecoMac) {
        if (conn == null) {
            System.out.println("Conexão com o banco não inicializada!!!!!!!!!!");
            return 0;
        }
        try {
            String sqlSelect = """
                SELECT 
                    maquina.idMaquina, 
                    maquina.enderecoMac, 
                    maquina.fkEmpresa,
                    empresa.nome 
                FROM bitware_db.Maquina AS maquina 
                INNER JOIN bitware_db.Empresa AS empresa 
                ON maquina.fkEmpresa = empresa.idEmpresa 
                WHERE maquina.enderecoMac = ?;
                """;

            PreparedStatement ps = conn.prepareStatement(sqlSelect);


            ps.setString(1, enderecoMac.toString());

            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
              //  int idMaquinaQuery = rs.getInt("idMaquina");
                int fkEmpresaQuery = rs.getInt("fkEmpresa");
                String nomeEmpresaQuery = rs.getString("nome");

                System.out.println("Máquina com o endereço MAC " + enderecoMac + " existe no banco de dados!");
               // System.out.println("ID da máquina: " + idMaquinaQuery);
                System.out.println("Pertence à empresa (id): " + fkEmpresaQuery + " - " + nomeEmpresaQuery);
                System.out.println();
                rs.close();
                ps.close();
                return fkEmpresaQuery;
            } else {
                rs.close();
                ps.close();
                return 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Ocorreu um erro ao tentar selecionar a máquina!!!!!!!!!");
            return 0;
        }
    }
}
