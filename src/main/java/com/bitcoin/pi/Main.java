package com.bitcoin.pi;

public class Main {
    public static void main(String[] args) {
        MySql bd = new MySql("bitware_db", "sysadmim", "1234");
        bd.conectBD();

    }
}