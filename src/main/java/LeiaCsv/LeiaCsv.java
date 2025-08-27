package LeiaCsv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class LeiaCsv {

    public static void main(String[] args) {
        String csvFile = "src/main/java/LeiaCsv/ArquivoSistemaOperacional.csv";
        String line;
        String csvSep = ";";

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {

            while ((line = reader.readLine()) != null) {
                String[] data = line.split(csvSep);


                if (data.length >= 4) {
                    String nome = data[0];
                    String cpu = data[1];
                    String memoria = data[2];
                    String ram = data[3];

                    System.out.println(
                                    "Nome: " + nome +
                                    "  CPU: " + cpu +
                                    "  Memória: " + memoria +
                                    "  RAM: " + ram
                    );
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
