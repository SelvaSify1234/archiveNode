
var http = require('http')
var express = require('express')
const bodyParser = require("body-parser");
const mysql_import = require('mysql-import');
var mysql = require('mysql');
var helmet = require("helmet");
var cors = require('cors');
var qs = require('qs');
var logger =require('simple-node-logger');
var dateFormat = require('dateformat');
var fs = require('fs');


var app = express()
app.use(bodyParser.json());
app.use(helmet());
app.use(cors());
app.use(bodyParser.urlencoded({extended : true}));


var name =dateFormat(new Date(), "yyyy-MM-dd"); 
var filename = 'logs/dbarchive_'+name+'.log';
fs.open(filename, 'w', function (err, file) {
  if (err) throw err;
  log.info(filename+' Log file has created !');
});

//create a logger file
const log = require('simple-node-logger').createSimpleLogger(filename);

log.info('subscription to ', 'channel', ' accepted at ', new Date().toJSON());

// respond with "hello world" when a GET request is made to the homepage
app.get('/', function (req, res) {
  res.send('hello world')
})

app.get('/app', function (req, res) {
    res.send('Testing App')
})


  // Post Method 
  app.post('/db/archive-manual',function(req,res,next){
  console.log(req.body);
  console.log(req.body['database_config'][1]);
//   var reqBody = req.body;
//  reqBody = JSON.parse(reqBody);
//  console.log(reqBody);
  console.log(req.body['database_config']['mydb']['mysql_source_host']);

  //var dbname = req.body['database_config']['mydb']['mysql_source_host'];
    //console.log(req.body['table_config']);
    log.info('Source Database Connection Check Start..');
  var connection = mysql.createConnection({
  host: req.body['database_config']['mydb']['mysql_source_host'],
  user: req.body['database_config']['mydb']['mysql_source_username'],
  password:req.body['database_config']['mydb']['mysql_source_password'],
  database: req.body['database_config']['mydb']['mysql_source_database'],
});

 connection.connect(function(err) {
  log.info('Check Source Database Connection');
      if (err) 
      {log.error('Source Database Not Connected ','some service ',+ err);}
      else
      {
      log.info('Source Database Connected Successfully.');
      }
  });

  log.info('Destination Database Connection Check Start..'); 

  var destiConnction = mysql.createConnection({
    host: req.body['database_config']['mydb']['mysql_destination_host'],
  user: req.body['database_config']['mydb']['mysql_destination_username'],
  password:req.body['database_config']['mydb']['mysql_destination_password'],
  database: req.body['database_config']['mydb']['mysql_destination_database'],
  });

  destiConnction.connect(function(err) {
    log.info('Check Destination Database Connection');
    if (err) 
    {log.error('Destination Database Not Connected ','some service ',+ err.message);}
    else
    {
    log.info('Destination Database Connected Successfully.');}
    });

    res.send("DB Check Done..");
     
  }).listen(3000);


