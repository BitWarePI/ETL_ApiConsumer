package com.bitcoin.pi.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BitwareDatabase {

    private static String url = "jdbc:mysql://localhost:3306/bitware_db";
    private static String user = "bitware";
    private static String password = "sptech";
//    private static String url = "jdbc:mysql://54.224.44.26:3306/bitware_db";
//    private static String user = "bitware";
//    private static String password = "sptech";

    private Connection conn;

    public BitwareDatabase() {
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao conectar no DB: " + e.getMessage(), e);
        }
    }

    public void close() {
        try { if (conn != null && !conn.isClosed()) conn.close(); } catch (SQLException ignored) {}
    }

    // Retorna idMaquina pelo enderecoMac (ou 0 se não existir)
    public int getFkMaquinaPeloMac(String mac) {
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

    // Busca parametro por fkMaquina + componente
    // Retorna Integer valor ou null se não existir.
    public Integer getParametro(int fkEmpresa, int fkMaquina, String componenteDescricao) {
        Integer idComponente = getIdComponenteByDescricao(componenteDescricao);
        if (idComponente == null) return null;
        String sql = "SELECT p.valor\n" +
                "        FROM Empresa e\n" +
                "        JOIN Maquina m ON m.fkEmpresa = e.idEmpresa\n" +
                "        JOIN Parametro p ON p.fkMaquina = m.idMaquina\n" +
                "        JOIN Componente c ON c.idComponente = p.fkComponente\n" +
                "        WHERE e.idEmpresa = ? AND m.idMaquina = ? AND c.descricao = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, fkEmpresa);
            ps.setInt(2, fkMaquina);
            ps.setString(3, componenteDescricao);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt("valor");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Busca parametro geral por fkEmpresa + componente (ParametrosGeraisEmpresa)
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


     // Método que prioriza buscar parâmetro através da fk_empresa
     // retorna Integer ou null
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

    public Integer getIdEmpresaPorMacAddress(String macAddress) {
        String sql = "SELECT fkEmpresa FROM Maquina WHERE enderecoMac = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, macAddress);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException e) { e.printStackTrace(); }
        return null;
    }

    // Cria chamado no banco; idTecnico pode ser null.
    public void criarChamado(int fkMaquina, String problema, String prioridade, String status, Integer idTecnico, String datetimeCsv) {
        String sql = "INSERT INTO Chamado (fkMaquina, problema, prioridade, status, idTecnico, dataAbertura) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, fkMaquina);
            ps.setString(2, problema);
            ps.setString(3, prioridade);
            ps.setString(4, status);
            if (idTecnico == null) ps.setNull(5, Types.INTEGER); else ps.setInt(5, idTecnico);
            ps.setString(6, datetimeCsv);
            ps.executeUpdate();
        } catch (SQLException e) { e.printStackTrace(); }
    }

    public List<Map<String, String>> listarChamadosNaoSincronizados() {
        List<Map<String, String>> chamados = new ArrayList<>();
        String sql = "SELECT idChamado, problema, prioridade FROM Chamado WHERE status = 'Aberto' AND (sincronizado = 0 OR sincronizado IS NULL)";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, String> c = new HashMap<>();
                c.put("id", String.valueOf(rs.getInt("idChamado")));
                c.put("problema", rs.getString("problema"));
                c.put("prioridade", rs.getString("prioridade"));
                chamados.add(c);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return chamados;
    }

    public List<Map<String, String>> pegarMacAdresses() {
        List<Map<String, String>> chamados = new ArrayList<>();
        String sql = "SELECT idChamado, problema, prioridade FROM Chamado WHERE status = 'Aberto' AND (sincronizado = 0 OR sincronizado IS NULL)";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, String> c = new HashMap<>();
                c.put("id", String.valueOf(rs.getInt("idChamado")));
                c.put("problema", rs.getString("problema"));
                c.put("prioridade", rs.getString("prioridade"));
                chamados.add(c);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return chamados;
    }

    public int getFkEmpresaPeloMac(String mac) {
        String sql = "SELECT fkEmpresa FROM Maquina WHERE enderecoMac = ?";
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

    public void marcarChamadoComoSincronizado(int idChamado) {
        String sql = "UPDATE Chamado SET sincronizado = 1 WHERE idChamado = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, idChamado);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Individual Isaak (Desafio técnico)

    // Busca id's das empresas
    public List<Integer> listarIdsEmpresas() {
        List<Integer> lista = new ArrayList<>();
        String sql = "SELECT idEmpresa FROM Empresa";

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                lista.add(rs.getInt("idEmpresa"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return lista;
    }

    // Buscar ocorrências pelo empresa
    public List<Map<String, String>> buscarOcorrencias(int fkEmpresa){
        List<Map<String, String>> chamados = new ArrayList<>();
        String sql = "SELECT\n" +
                "            Chamado.idChamado,\n" +
                "            Chamado.fkMaquina,\n" +
                "            Chamado.problema,\n" +
                "            Chamado.prioridade,\n" +
                "            Chamado.status,\n" +
                "            Chamado.idTecnico,\n" +
                "            Chamado.dataAbertura,\n" +
                "            Chamado.sincronizado,\n" +
                "            Maquina.nome AS nomeMaquina,\n" +
                "            Maquina.enderecoMac AS macMaquina\n" +
                "        FROM\n" +
                "            Chamado\n" +
                "        JOIN\n" +
                "            Maquina ON Chamado.fkMaquina = Maquina.idMaquina\n" +
                "        JOIN\n" +
                "            Empresa ON Maquina.fkEmpresa = Empresa.idEmpresa\n" +
                "        WHERE\n" +
                "            Empresa.idEmpresa = ?\n" +
                "        ORDER BY\n" +
                "            Chamado.dataAbertura DESC;";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, fkEmpresa);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, String> c = new HashMap<>();
                    c.put("idChamado", String.valueOf(rs.getInt("idChamado")));
                    c.put("fkMaquina", String.valueOf(rs.getInt("fkMaquina")));
                    c.put("problema", rs.getString("problema"));
                    c.put("prioridade", rs.getString("prioridade"));
                    c.put("status", rs.getString("status"));
                    c.put("dataAbertura", rs.getString("dataAbertura"));
                    c.put("nomeMaquina", rs.getString("nomeMaquina"));
                    c.put("enderecoMac", rs.getString("macMaquina"));
                    chamados.add(c);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return chamados;
    }
}