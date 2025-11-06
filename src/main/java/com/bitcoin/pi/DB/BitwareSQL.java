package com.bitcoin.pi.DB;

import java.sql.*;

/**
 * Implementação simplificada do acesso ao MySQL.
 * Ajuste JDBC_URL, USER e PASS conforme seu ambiente.
 */
public class BitwareSQL {

    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/bitware_db";
    private static final String USER = "root";
    private static final String PASS = "Home27659317@";
//    private static final String JDBC_URL = "jdbc:mysql://54.224.44.26:3306/bitware_db";
//    private static final String USER = "bitware";
//    private static final String PASS = "sptech";

    private Connection conn;

    public BitwareSQL() {
        try {
            conn = DriverManager.getConnection(JDBC_URL, USER, PASS);
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao conectar no DB: " + e.getMessage(), e);
        }
    }

    public void close() {
        try { if (conn != null && !conn.isClosed()) conn.close(); } catch (SQLException ignored) {}
    }

    /**
     * Retorna idMaquina pelo enderecoMac (ou 0 se não existir)
     */
    public int getFkMaquinaByMac(String mac) {
        String sql = "SELECT idMaquina FROM Maquina WHERE enderecoMac = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mac);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Busca parametro por fkMaquina + componente (conforme sua tabela Parametro).
     * Retorna Integer valor ou null se não existir.
     */
    public Integer getParametro(int fkMaquina, String componenteDescricao) {
        // precisamos mapear componenteDescricao -> idComponente (pela tabela Componente)
        Integer idComponente = getIdComponenteByDescricao(componenteDescricao);
        if (idComponente == null) return null;
        String sql = "SELECT valor FROM Parametro WHERE fkMaquina = ? AND fkComponente = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, fkMaquina);
            ps.setInt(2, idComponente);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException e) { e.printStackTrace(); }
        return null;
    }

    /**
     * Busca parametro gerencial por fkEmpresa + componente (ParametrosGeraisEmpresa)
     * retorna Integer ou null
     */
    public Integer getParametroGeraisEmpresa(int fkEmpresa, String componenteDescricao) {
        String sql = "SELECT cpu_percent, gpu_percent, cpu_temperature, gpu_temperature FROM ParametrosGeraisEmpresa WHERE fkEmpresa = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, fkEmpresa);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    switch (componenteDescricao) {
                        case "cpu_percent": return rs.getInt("cpu_percent");
                        case "gpu_percent": return rs.getInt("gpu_percent");
                        case "cpu_temperature": return rs.getInt("cpu_temperature");
                        case "gpu_temperature": return rs.getInt("gpu_temperature");
                        default: return null;
                    }
                }
            }
        } catch (SQLException e) { e.printStackTrace(); }
        return null;
    }

    /**
     * Método que prioriza buscar parâmetro através da fk_empresa (como você pediu)
     * retorna Integer ou null
     */
    public Integer getParametroByEmpresa(int fkEmpresa, String componenteDescricao) {
        return getParametroGeraisEmpresa(fkEmpresa, componenteDescricao);
    }

    private Integer getIdComponenteByDescricao(String descricao) {
        String sql = "SELECT idComponente FROM Componente WHERE descricao = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, descricao);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException e) { e.printStackTrace(); }
        return null;
    }

    /**
     * Cria chamado no banco; idTecnico pode ser null.
     */
    public void criarChamado(int fkMaquina, String problema, String prioridade, String status, Integer idTecnico) {
        String sql = "INSERT INTO Chamado (fkMaquina, problema, prioridade, status, idTecnico) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, fkMaquina);
            ps.setString(2, problema);
            ps.setString(3, prioridade);
            ps.setString(4, status);
            if (idTecnico == null) ps.setNull(5, Types.INTEGER); else ps.setInt(5, idTecnico);
            ps.executeUpdate();
        } catch (SQLException e) { e.printStackTrace(); }
    }
}
