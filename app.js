

var fs = require('fs');
const bodyParser = require("body-parser");
var mysql = require('mysql');
var helmet = require("helmet");
var cors = require('cors');
const express = require('express');
var archive = require('./forumnxt_archive');
var log = require('./log');


var sour_host;
var sour_port;
var sour_db;
var dest_host;
var dest_port;
var dest_db;

var create_table;
var module_name;
var dbkeys;
var customer_type;
var dest_con;
var sour_con;

var app = express();
app.use(bodyParser.json());
app.use(helmet());
app.use(cors());
app.use(bodyParser.urlencoded({extended : true}));




/* Archive Data Post Call*/
  app.post('/db/archive-manual',function(req,res,next){

    log.create_log();

   dbkeys =Object.keys(req.body.database_config);

   for (var key in req.body['database_config'][dbkeys]) {
    eval(key+ " = req.body['database_config'][dbkeys][key]");
  }

  sour_con = mysql.createConnection({
    host:mysql_source_host,
  user: mysql_source_username,
  password:mysql_source_password,
  database: mysql_source_database
   });
  
   customer_type = customer_type;
   sour_host = mysql_source_host;
   dest_host=mysql_destination_host;
   sour_db=mysql_source_database;
   dest_db=mysql_destination_database;
   sour_port= mysql_source_port;
   dest_port=mysql_destination_port;
  create_table=create_dest_table_if_not_exists;
  module_name=mysql_select_module;
  
   sour_con.connect(function(err) {
    log.info('Check Source Database Connection Check');
       if (err !=null) 
       {log.info('Source Database Not Connected . Please make sure Source Database configuration valid.');
         response('Source Database Not Connected . Please make sure Source Database configuration valid.');}
       else
       {
             log.info('Source Database Connected Successfully.');
       }
    });

    dest_con =mysql.createConnection({
        host: mysql_destination_host,
        user: mysql_destination_username,
        password:mysql_destination_password,
       database: mysql_destination_database
    });

    dest_con.connect(function(err) {
        log.info('Check Destination Database Connection Check.');
        if (err !=null) 
        {log.error('Destination Database Not Connected !.. Please make sure Destination Details  ');
        response('Destination Database Not Connected !.. Please Make sure Destination Details  ');}
        else
        {          log.info('Destination Database Connected Successfully.');        }
    });
    /* Archive Data For FourmNXT Type */ 
    if(customer_type=='1')
    {
      var p$=archive.do_archive(sour_con,dest_con,create_table,module_name,sour_db,dest_db,sour_host,dest_host,sour_port,dest_port).then(
        stat => {
          if(stat){
           log.info('Data Archive has been done.');
           response ('Data Archive has been done.');
          }
          else{
           // response ('Some Reasone Data Archive has been NOT Done.');
        //  log.info('Data Archive has been done.');
        //  response ('Data Archive has been done.');
          }
        }
      )
.catch(err=>{

    console.log('\n----------------------\n');
    console.log(err.message);-
    console.log('\n----------------------\n');

    log.error('\n----------------------\n');
    log.error(err.message);-
    log.error('\n----------------------\n');
    res.setHeader('Content-Type', 'application/json');
    res.send({message: err.message });
    res.end;
     })
.finally( _ => {
  
   // sour_con.end();
    //dest_con.end();
  });
 
    }
  
    else{          /* Archive Data For Others Type */     }


function response (message)
{
  res.setHeader('Content-Type', 'application/json');
  res.send({message: message });
  res.end;
}   
});


  app.listen(3000, () => {  });

module.exports =app;
