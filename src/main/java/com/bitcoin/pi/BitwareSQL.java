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
            String user = "aluno";
            String password = "123456";

            this.conn = DriverManager.getConnection(url, user, password);
            System.out.println("Conectado ao MySQL com sucesso!!!!!!!!!!!!!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean CompararBanco(Integer idMaquina, String enderecoMac, Integer fkEmpresa) {
        if (conn == null) {
            System.out.println("Conexão com o banco não inicializada!!!!!!!!!!");
            return(false);
        }
        try {
            String sqlSelect = "SELECT * FROM Maquina WHERE idMaquina = ? and enderecoMac = ? and fkEmpresa = ?;";
            PreparedStatement ps = conn.prepareStatement(sqlSelect);

            ps.setString(1, idMaquina.toString());
            ps.setString(2, enderecoMac);
            ps.setString(3, fkEmpresa.toString());

            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                System.out.println("Máquina com o endereço mac "+enderecoMac+" existe no banco de dados!!!!!\n\n");
                rs.close();
                ps.close();
                return(true);
//                System.out.println("Maquina encontrada:");
//                System.out.printf("MÁQUINA QUE EXISTE: | id da máquina: %d | endereço MAC: %s | id da empresa: %d",
//                        rs.getInt("idMaquina"),  rs.getString("enderecoMac"),  rs.getInt("fkEmpresa"));
            } else {
                System.out.println("Máquina com o endereço mac " +enderecoMac+ " não encontrada!!!!!!\n\n");
                rs.close();
                ps.close();
                return(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Ocorreu um erro ao tentar selecionar a máquina!!!!!!!!!");
            return(false);
        }
    }
}
