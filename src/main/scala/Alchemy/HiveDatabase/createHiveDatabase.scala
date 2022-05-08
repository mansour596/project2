package Alchemy.HiveDatabase

import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

object createHiveDatabase {
    def makeDatabase(): Unit = {
        var con: java.sql.Connection = null
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/Ecommerce"

            Class.forName(driverName)

            con = DriverManager.getConnection(conStr, "", "")
            val stmt = con.createStatement()
            stmt.execute("CREATE DATABASE IF NOT EXISTS Ecommerce")
            System.out.println("Database created successfully")
        }
}