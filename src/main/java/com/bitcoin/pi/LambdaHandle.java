package com.bitcoin.pi;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.Map;

public class LambdaHandle implements RequestHandler<Map<String, Object>, String> {

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        context.getLogger().log("Lambda ETL rodando!\n");

        try {
            // Iniciando o main que está no ProcessadorS3
            ProcessadorS3.main(new String[]{});

            context.getLogger().log("Execução do ETL concluída com sucesso!\n");
            return "Execução concluída com sucesso!";
        } catch (Exception e) {
            context.getLogger().log("Erro durante execução do ETL: " + e.getMessage() + "\n");
            e.printStackTrace();
            return "Falha ao executar ETL: " + e.getMessage();
        }
    }
}
