var fs = require('fs');
const bodyParser = require("body-parser");
var mysql = require('mysql');
var helmet = require("helmet");
var cors = require('cors');
const express = require('express');
var archive = require('./forumnxt_archive');
var log = require('./log');
var forEach = require('async-foreach').forEach;


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

var sel_query;
var del_query;
var sour_table;
var dest_table;
var sequence = 0;
var post = 0;
global.msg = '';
let flg_sor = 0;

var app = express();
app.use(bodyParser.json());
app.use(helmet());
app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));




/* Archive Data Post Call*/
app.post('/db/archive-manual', function(req, res, next) {
    
    log.create_log();
    dbkeys = Object.keys(req.body.database_config);

    for (var key in req.body['database_config'][dbkeys]) {
        eval(key + " = req.body['database_config'][dbkeys][key]");
    }

    sour_con = mysql.createConnection({ host: mysql_source_host, user: mysql_source_username, password: mysql_source_password, database: mysql_source_database });
    dest_con = mysql.createConnection({ host: mysql_destination_host, user: mysql_destination_username, password: mysql_destination_password, database: mysql_destination_database });
    customer_type = customer_type;
    sour_host = mysql_source_host;
    dest_host = mysql_destination_host;
    sour_db = mysql_source_database;
    dest_db = mysql_destination_database;
    sour_port = mysql_source_port;
    dest_port = mysql_destination_port;
    create_table = create_dest_table_if_not_exists;
    module_name = mysql_select_module;


    source_conn(sour_con, dest_con)
        .then(rst => {
            
            if (rst) {

                if (customer_type == '1' && sour_con.state == 'authenticated' && dest_con.state == 'authenticated') 
                {
                    get_sales_data(sour_con, module_name, sour_db)
                        .then(result => {
                            if (result.length > 0) 
                            {
                                forEach(result, function(item, index, arr) {
                                    sel_query = item['sel_query'];
                                    del_query = item['del_query'];
                                    sequence = item['vt_tabid'];
                                    sour_table = sel_query.match(new RegExp('FROM' + "(.*)" + 'WHERE'))[1];
                                    sour_table = sour_table.substring(0, sour_table.indexOf("WHERE"));
                                    sour_table = sour_table.replace(/\s/g, "");
                                    dest_table = sour_table + '_archival';

                                    /* Do Archive */
                                    archive.do_archive(sour_con, dest_con, create_table, module_name, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query, del_query, sequence)
                                        .then(stat => {
                                            if (stat) 
                                            {
                                                log.info('Data Archive has been done.');                                                
                                               
                                                 if(index == result.length-1)
                                                  {
                                                  res.setHeader('Content-Type', 'application/json');
                                                  res.send({ message: 'Data archive process has been completed.' });
                                                  res.end();
                                                  }

                                            } else {
                                                log.info('Some reasone data archive has been NOT Done.');
                                                msg = 'Some reasone data archive has been NOT Done.';
                                            }
                                        })
                                        .catch(err => {
                                            log.error('\n----------------------\n');
                                            msg += 'Module Name : '+ module_name +' Row No :'+ index + 'Error :'+ err.message;
                                            log.error(err.message);
                                            log.error('\n----------------------\n');
                                            
                                            log.log_entry(sour_con, sequence, module_name, '2', sour_db);
                                          if(err.message != null && index == result.length-1)
                                          {
                                           res.setHeader('Content-Type', 'application/json');
                                            res.send({ message: msg });
                                             res.end();
                                             msg='';
                                          }
                                        });
                                });
                            }
                        })
                        .catch(err => {
                            post = 1;
                            log.error('\n----------------------\n');
                            log.error(err.message);
                            log.error('\n----------------------\n');
                            if (sequence != 0)
                                log.log_entry(sour_con, sequence, module_name, '2', sour_db);
                            res.setHeader('Content-Type', 'application/json');
                            res.send({ message: err.message });
                            res.end();                          
                        })
                        // .finally(_ => {
                        //   console.log('Get data finally');
                        //    });

                } else { /* Archive Data For Others Type */ }
            }
        })
        .catch(err => {
            log.error('\n----------------------\n');
            log.error(err.message);
            log.error('\n----------------------\n');
            res.setHeader('Content-Type', 'application/json');
            res.send({ message: err.message });
            res.end();
        });
   });


async function get_sales_data(sour_con, name, sour_db) {
    return new Promise((rs, rj) => {
        var sql = 'select * from sify_darc_modules_query where module_name="' + name + '"  order by sequence asc';
        sour_con.query(sql, function(err, result, fields) {
            if (err || !result.length > 0) {
                rj(new Error('Module Name is not valide. Module Name data not exist. '));
                log.info('Module Name is not valide. Module Name data not exist');
                return false;
            } else {
                if (result.length > 0)
                    rs(result);
            }
        });
    });
}


async function source_conn(sour_con, dest_con) {
    return new Promise((rs, rj) => {
        sour_con.connect(function(err, result) {
            if (err != null) {
                rj(new Error('Source database not connected !.. Please make sure source database configuration valid.'));
                return false;
              
            } else {
               log.info('Source database connected successfully.');
                dest_con.connect(function(err) {
                    if (err != null) {
                        rj(new Error('Destination database not connected !.. Please make sure destination Details '));
                        return false;
                     
                    } else {
                        log.info('Destination database connected successfully.');
                        rs(true);
                    }
                });
            }
        });
    });
}



app.listen(3000, () => {});

module.exports = app;