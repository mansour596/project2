package Alchemy.HiveTable

import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

object createHiveTable {

    def makeTable(): Unit ={
        var con: java.sql.Connection = null;
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/Ecommerce"

        Class.forName(driverName)
        con = DriverManager.getConnection(conStr, "", "")
        val stmt = con.createStatement()


        val tableName = "Alchemy";
        println(s"Dropping table $tableName..")
        stmt.execute("drop table IF EXISTS " + tableName)
        println(s"Creating table and adding data $tableName..")
        stmt.execute("create external table " + tableName + "(order_id int,customer_id int,customer_name string,prod_id int,product_name string,product_category string,payment_type string,qty int,price float,datetime string,country string,city string,ecommerce_website_name string,payment_txn_id int,payment_txn_success string,failure_reason string) row format delimited  fields terminated by ',' location 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/'")  
        println("Tables Created with data added!")

       
    }
}