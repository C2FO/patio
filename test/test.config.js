var MYSQL_URI = "mysql://test:testpass@localhost:3306";
var PG_URI = "pg://test:testpass@localhost:5432";
var DB_URI = process.env.PATIO_DB === "pg" ? PG_URI : MYSQL_URI;

module.exports = {
    MYSQL_URI:MYSQL_URI,
    PG_URI:PG_URI,
    DB_URI: DB_URI
};