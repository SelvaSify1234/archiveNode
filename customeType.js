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

var module_name;

var sorce_table_schema;
var sel_query;
var del_query;
var soure_db;
var dest_db;
var slect_query;
var inser_data;

var app = express()
app.use(bodyParser.json());
app.use(helmet());
app.use(cors());
app.use(bodyParser.urlencoded({extended : true}));


var name =dateFormat(new Date(), "yyyy-MM-dd"); 
var file_name = 'logs/dbarchive_'+name+'.log';
fs.open(file_name, 'w', function (err, file) {
  if (err) throw err;
  log.info(file_name+' Log file has created !');
});

//create a logger file
const log = require('simple-node-logger').createSimpleLogger(file_name);

log.info('subscription to ', 'channel', ' accepted at ', new Date().toJSON());
// respond with "hello world" when a GET request is made to the homepage
app.get('/', function (req, res) {
  res.send('hello world')
})


 // Post Method 
 app.post('/db/archive-manual',function(req,res,next){

  var dbkeys=Object.keys(req.body.database_config);
  
   sorceConn();
   soure_db.connect(function(err) {
      log.info('Check Source Database Connection Check');
         if (err) 
         {log.error('Source Database Not Connected ','some service ',+ err.message);}
         else
         {
         log.info('Source Database Connected Successfully.');
     slect_query='select * from sify_darc_modules_query where module_name="'+ req.body['database_config'][dbkeys[0]]['mysql_select_module']  +'"  order by sequence asc';
    sourcSelectData();
    sourceTblSchema();    
    sourceData(); 
    }
 });
    
   

    destConn();
    dest_db.connect(function(err) {
      log.info('Check Destination Database Connection Check.');
      if (err) 
      {log.error('Destination Database Not Connected !.. Please Make sure Destination Details  ','some service ',+ err.message);
      response('Destination Database Not Connected !.. Please Make sure Destination Details  ','some service ',+ err.message );}
      else
      {
        log.info('Destination Database Connected Successfully.');
              // if the source and destination servers are running on same instances i.e same port
           if(req.body['database_config'][dbkeys[0]]['mysql_source_host']!=req.body['database_config'][dbkeys[0]]['mysql_destination_host'] && req.body['database_config'][dbkeys[0]]['mysql_source_port']!=req.body['database_config'][dbkeys[0]]['mysql_destination_port'] )
           {
    
            console.log('Same Server');
            if(req.body['database_config'][dbkeys[0]]['create_dest_table_if_not_exists']==true)
            {
           // log.info('Destination Table Checking..');
           console.log('Table Exist Check Server');
            dest_db.query('SHOW TABLES LIKE "vtiger_salesinvoice_archival"', (err, results) => {
           //  console.log(err.message+' Test');
                if (err ==null ) 
                {log.error('Destination Table Not Exist . New Table Create Start..' ); 
              
                  console.log('Destinatation Table Create');
                var create_desti_table='CREATE TABLE vtiger_salesinvoice_archival LIKE '+req.body['database_config'][dbkeys[0]]['mysql_source_database']+'.vtiger_salesinvoice';
                dest_db.query(create_desti_table, function(err,result,fields){
                  if (err) 
                  {log.error('Destination Table Not Created ' ,+ err.message);
                  response('Destination Table Not Created'+err.message  );
                 }
                  else{
                      log.info('Destination Table Has Created Successfully.. ');
   
                      var dest_insert='INSERT INTO vtiger_salesinvoice_archival '+sel_query;
                     
                      dest_db.query(dest_insert, function(err,result,fields){
                       if (err) 
                       {log.error('Destination Data Not Inserted  On Destination Table' ,+ err.message);
                       response('Destination Data Not Inserted  On Destination Table' );
                     }
                       else{
                           log.info('Destination Table Data Inserted Successfully.. ');
                         
                           soure_db.query(del_query, function(err,result,fields){
                             if (err) 
                             {log.error('Source Table Data Not Deleted' ,+ err.message);
                             response('Source Table Data Not Deleted'+err.message ); 
                          }
                             else{
                                 log.info('Source Table Data Deleted Successfully.. ');
                                 response('Archive process successfully done..'); 
                               }
                             });
                         }
                       });
                  }  
              });
              }
              else {
                console.log('Same  Not Create table insert exist table');
                var dest_insert='INSERT INTO vtiger_salesinvoice_archival '+sel_query;
                dest_db.query(dest_insert, function(err,result,fields){
                  if (err) 
                  {log.error('Destination Data Not Inserted  On Destination Table' ,+ err.message);
                  response('Destination Data Not Inserted  On Destination Table' +err.message);                }
                  else{
                      log.info('Destination Table Data Inserted Successfully.. ');
                    
                      soure_db.query(del_query, function(err,result,fields){
                        if (err) 
                        {log.error('Source Table Data Not Deleted' ,+ err.message);
                        response('Source Table Data Not Deleted' +err.message);    }
                        else{
                            log.info('Source Table Data Deleted Successfully.. ');
                            response('Source Table Data Deleted Successfully..'); 
                          }
                        });
                    }
                  });

              }
            });
          }
                else{             
                  console.log('Same  server No Table');
                 // dest_db.query('SHOW TABLES LIKE "vtiger_salesinvoice_archival"', (err, results) => {
                  var sql = 'select * from vtiger_salesinvoice_archival';
                  dest_db.query(sql, (err, results) => {
                    if (err!=null) 
                    {log.error('Destination table not exist . Pelase choose Create table if not exists options' ,'some service ');
                   // console.log('Destination table not available');
                   response('Destination table not exist . Pelase choose Create table if not exists options'); 
                    }
                     else{
                      log.info('table not check.. directly insert..');
                      var dest_insert='INSERT INTO vtiger_salesinvoice_archival '+sel_query;
                    
                dest_db.query(dest_insert, function(err,result,fields){
                  if (err) 
                  {log.error('Destination Data Not Inserted  On Destination Table' ,+ err.message);
                  response('Destination Data Not Inserted  On Destination Table'+err.message);     
                  }
                  else{
                      log.info('Destination Table Data Inserted Successfully.. ');
                    
                      soure_db.query(del_query, function(err,result,fields){
                        if (err) 
                        {log.error('Source Table Data Not Deleted' ,+ err.message);
                        res.send({ message: 'Source Table Data Not Deleted '+err.message })
                        res.end;}
                        else{
                            log.info('Source Table Data Deleted Successfully.. ');
                            response('Source Table Data Deleted Successfully.. ');                           
                          }
                        });
                    }
                  });
                      }
                });
           
                }
              }
           
         
           else{
               // if the source and destination servers are running on Different  instances i.e same port
                 //Create the destination table here
                // sorce_table_schema = sorce_table_schema.replace('vtiger_salesinvoice','vtiger_salesinvoice_archival');
   //console.log(sorce_table_schema);
  
   console.log('Diffrent Source');
   
            // var create_desti_table='CREATE TABLE vtiger_salesinvoice_archival LIKE '+req.body['database_config'][dbkeys[0]]['mysql_source_database']+'.vtiger_salesinvoice';
            sourcSelectData();           
            sourceTblSchema();
            sourceData();
            bulkDataUpload();
            console.log('Test'+sorce_table_schema);
            console.log('Diffrent Source');
          //  console.log(sel_query);
            dest_db.query(sorce_table_schema, function(err,result,fields){
                   if (err.message !='ER_EMPTY_QUERY: Query was empty') 
                   {log.error('Destination table not created'+err.message );
                  // response('Destination table not created '+err.message );
                  }
                   else{
                 //   console.log('Im In' + sorce_table_schema +'  '+sel_query);
                      //  log.info('Destination table has been created successfully.. ');
                      
                      //  var dest_insert='INSERT INTO vtiger_salesinvoice_archival '+sel_query;
                                            
                      //  dest_db.query(dest_insert, function(err,result,fields){
                      //   if (err) 
                      //   {log.error('Destination Data Not Inserted  On Destination Table' ,+ err.message);
                      //   response('Destination Data Not Inserted  On Destination Table' );
                      // }
                      //   else{
                      //       log.info('Destination Table Data Inserted Successfully.. ');
                          
                      //       soure_db.query(del_query, function(err,result,fields){
                      //         if (err) 
                      //         {log.error('Source Table Data Not Deleted' ,+ err.message);
                      //         response('Source Table Data Not Deleted'+err.message ); 
                      //      }
                      //         else{
                      //             log.info('Source Table Data Deleted Successfully.. ');
                      //             response('Archive process successfully done..'); 
                      //           }
                      //         });
                      //     }
                      //   });


                   }  
               });
           }               
            }          
        });
   //}
//      else{
// // if the source and destination servers are running on same instances i.e same port
// if(req.body['database_config'][dbkeys[0]]['mysql_source_host']==req.body['database_config'][dbkeys[0]]['mysql_destination_host'] && req.body['database_config'][dbkeys[0]]['mysql_source_port']==req.body['database_config'][dbkeys[0]]['mysql_destination_port'] )
// {
  
// var create_desti_table='CREATE TABLE vtiger_salesinvoice_archival LIKE '+req.body['database_config'][dbkeys[0]]['mysql_source_database']+'.vtiger_salesinvoice';

// // var delete_source_table_data ='DELETE FROM table_name WHERE =( '+del_query+' )';
// console.log(create_desti_table);
// dest_db.query(create_desti_table, function(err,result,fields){
//     if (err) 
//     {log.error('Destination Table Not Created ' ,+ err.message);}
//     else{
//         log.info('Destination Table Has Created Successfully.. ');

//         var dest_insert='INSERT INTO vtiger_salesinvoice_archival '+sel_query;
       
//         dest_db.query(dest_insert, function(err,result,fields){
//          if (err) 
//          {log.error('Destination Data Not Inserted  On Destination Table' ,+ err.message);}
//          else{
//              log.info('Destination Table Data Inserted Successfully.. ');
           
//              console.log(del_query);
//              soure_db.query(del_query, function(err,result,fields){
//                if (err) 
//                {log.error('Source Table Data Not Deleted' ,+ err.message);}
//                else{
//                    log.info('Source Table Data Deleted Successfully.. ');
//                    res.send("DB Check Done..");}
//                });
           
//            }
//          });
//     }  
// });
// }
//      }
//  }
//       });


// res.setHeader('Content-Type', 'application/json');
// res.send({ data: 'Destination Table Not Exist . Pelase choose create table options' })
//   res.end;
function sorceConn()
{
  console.log('Source Connection');
  log.info('Source Database Connection Check Start..');
  soure_db = mysql.createConnection({
  host: req.body['database_config'][dbkeys[0]]['mysql_source_host'],
  user: req.body['database_config'][dbkeys[0]]['mysql_source_username'],
  password:req.body['database_config'][dbkeys[0]]['mysql_source_password'],
  database: req.body['database_config'][dbkeys[0]]['mysql_source_database']
});
}

function destConn()
{
  log.info('Destination Database Connection Check Start..'); 
     dest_db = mysql.createConnection({
     host: req.body['database_config'][dbkeys[0]]['mysql_destination_host'],
    user: req.body['database_config'][dbkeys[0]]['mysql_destination_username'],
    password:req.body['database_config'][dbkeys[0]]['mysql_destination_password'],
   database: req.body['database_config'][dbkeys[0]]['mysql_destination_database'],
    });
}

function sourcSelectData()
{
  console.log('sourcSelectData');
  soure_db.query(slect_query, function(err,result,fields){
    if(err) 
    {log.error('Source Database Select Table error ','some service ',+ err.message);}
    else{
        sel_query = result[0]['sel_query'];
        del_query = result[0]['del_query'];
        sel_query = sel_query.replace(/vtiger_salesinvoice/g,  req.body['database_config'][dbkeys[0]]['mysql_source_database']  +'.vtiger_salesinvoice');
        del_query = del_query.replace(/vtiger_salesinvoice/g,  req.body['database_config'][dbkeys[0]]['mysql_source_database']  +'.vtiger_salesinvoice');
    }     
  console.log(JSON.stringify(sel_query));
});
}

function sourceTblSchema()
{
  console.log('sourceTblSchema');
  soure_db.query('SHOW CREATE TABLE vtiger_salesinvoice ', function(err,result,fields){
    if (err) {
    console.log(err)}
    else{
      sorce_table_schema = result[0]['Create Table'];
      sorce_table_schema =sorce_table_schema.replace(/vtiger_salesinvoice/g,'vtiger_salesinvoice_archival');
      console.log(sorce_table_schema);     
    }
  });
}


function sourceData()
{
  console.log('sourceData');
 // sourcSelectData();
  //soure_db.query(sel_query, function(err,result,fields){
    soure_db.query("SELECT * FROM mydb.vtiger_salesinvoice WHERE mydb.vtiger_salesinvoice.salesinvoiceid IN (SELECT mydb.vtiger_salesinvoice.salesinvoiceid FROM mydb.vtiger_salesinvoice INNER JOIN mydb.vtiger_salesinvoicecf on mydb.vtiger_salesinvoicecf.salesinvoiceid=mydb.vtiger_salesinvoice.salesinvoiceid WHERE TIMESTAMPDIFF(MONTH,mydb.vtiger_salesinvoicecf.cf_salesinvoice_sales_invoice_date,CURDATE()) > 3 AND mydb.vtiger_salesinvoice.status='Created');", function(err,result,fields){
    if (err) {
     // throw err;
      console.log('Testing'+ err.message);
    }
    else { 
      console.log(JSON.stringify(result));
      inser_data = JSON.stringify(result);
    }
  });
}

function bulkDataUpload()
{
  console.log('bulkDataUpload');
//   var sql = "INSERT INTO Test (name, email, n) VALUES :params";
// var values = [
//     ['demian', 'demian@gmail.com', 1],
//     ['john', 'john@gmail.com', 2],
//     ['mark', 'mark@gmail.com', 3],
//     ['pete', 'pete@gmail.com', 4]
// ];
var sql ="INSERT INTO vtiger_salesinvoice_archival VALUES ?";
//console.log(inser_data);
inser_data='[{"salesinvoiceid":1001,"cf_salesinvoice_sales_invoice_date":"2019-03-02T18:30:00.000Z","status":"Created"},{"salesinvoiceid":1002,"cf_salesinvoice_sales_invoice_date":"2019-03-02T18:30:00.000Z","status":"Created"}]';
dest_db.query(sql, inser_data, function(err,result) {
    if (err) 
    { console.log( err.message);}
    else{ console.log('bulkUpload Done .' );}
    
});
}

function response (message)
{
  res.setHeader('Content-Type', 'application/json');
    res.send({message: message });
  res.end;
}
    }).listen(3000);
  