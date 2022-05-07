//const mysql = require('mysql');
import mysql from 'mysql'
//CReate mysql Connection
const dbConn = mysql.createConnection({
    host:'carla.cdjlzszqac6g.us-east-1.rds.amazonaws.com',
    user:'admin',
    password:'ironMan#83',
    database: 'carla'
})

dbConn.connect(function(error){
    if(error) throw error;
    console.log("Database Connected Successfully")
})

//module.exports = dbConn;
export default dbConn ;