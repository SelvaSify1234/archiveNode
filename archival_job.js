var fs = require('fs');
const bodyParser = require("body-parser");
var mysql = require('mysql');
var helmet = require("helmet");
var cors = require('cors');
const express = require('express');
var archive = require('./job_forumnxt');
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
var rela_tables;
var condition;
var sequence = 0;
global.msg = '';
var app = express();

app.use(bodyParser.json());
app.use(helmet());
app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));



archive_job();

async function archive_job()
{
  log.create_log();
  sour_con = mysql.createConnection({
    host: 'forumnxt-data-archival.cjujx2esp70l.ap-south-1.rds.amazonaws.com'
    , user: 'GSKL2018'
    , password: 'GSKL2018'
    , database: 'GSKL2018'
  });
  dest_con = mysql.createConnection({
    host: 'forumnxt-data-archival.cjujx2esp70l.ap-south-1.rds.amazonaws.com'
    , user: 'GSKL2018'
    , password:'GSKL2018'
    , database:'GSKL2018'
  });
  customer_type = '1';
  sour_host = 'forumnxt-data-archival.cjujx2esp70l.ap-south-1.rds.amazonaws.com';
  dest_host = 'forumnxt-data-archival.cjujx2esp70l.ap-south-1.rds.amazonaws.com';
  sour_db = 'GSKL2018';
  dest_db = 'GSKL2018';
  sour_port = '3306';
  dest_port = '3306';
  create_table = true;
  module_name = 'All';

  source_conn(sour_con, dest_con)
    .then(rst => {
      if (rst) {
        /* Archive Data For Forumnxt Type */
        if (customer_type == '1' && sour_con.state == 'authenticated' && dest_con.state == 'authenticated') {
          get_sales_data(sour_con, module_name, sour_db)
            .then(result => {
              if (result.length > 0) {
                 msg = '';
                 log.info( ` ${result.length}  : Modules selected for archival process. `  );
                forEach(result, function (item, index, arr) {
                  sel_query = item['sel_query_template'];
                  del_query = item['del_query_template'];
                  sequence = item['vt_tabid'];
                  var str = sel_query.match(/WHERE\b/);
                  var query = sel_query.slice(0,str.index+5);
                  sour_table =query.match(new RegExp('FROM' + "(.*)" + 'WHERE'))[1];
                  sour_table = sour_table.replace(/\s/g, "");
                  dest_table = sour_table + '_archival';
                  /* Do Archive */
                  archive.do_archive(sour_con, dest_con, create_table, item['module_name'], sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query, del_query, item['vt_tabid'])
                    .then(stat => {
                      if (stat) {
                        log.info( ' Row No :' + index + '  ' + ' Data Archive has been done.');
                      }
                      else {
                      	log.info( ' Row No :' + index + '  ' + ' Some reasone data archival has been NOT Done.');
                      }
                    })
                    .catch(err => {
                      log.error('\n----------------------\n');
                      log.error(' Row No :' + index + '  ' + err.message);
                      log.error('\n----------------------\n');
                      log.log_entry(sour_con, item['vt_tabid'], item['module_name'], '2', sour_db, 0, 0,0, err.message.replace(/[^\w\s]/gi, ''));
                    });
                });
              }
            })
            .catch(err => {
              log.error('\n----------------------\n');
              log.error(' Row No :' + index + '  ' + err.message);
              log.error('\n----------------------\n');
              if (sequence != 0) log.log_entry(sour_con, item['vt_tabid'], item['module_name'], '2', sour_db, 0, 0,0, err.message.replace(/[^\w\s]/gi, ''));
            });
        }}
    })
    .catch(err => {
      log.error('\n----------------------\n');
      log.error(' Checking '+err.message);
      log.error('\n----------------------\n');
    });


async function source_conn(sour_con, dest_con) {
  return new Promise((rs, rj) => {
    sour_con.connect(function (err, result) {
      if (err != null) {
        rj(new Error('Source database not connected !.. Please make sure source database configuration is valid.'));
        return false;
      }
      else {
        log.info('Source database connected successfully.');
        dest_con.connect(function (err) {
          if (err != null) {
            rj(new Error('Destination database not connected !.. Please make sure source database configuration is valid.'));
            return false;
          }
          else {
            log.info('Destination database connected successfully.');
            rs(true);
          }
        });
      }
    });
  });
}

async function get_sales_data(sour_con, name, sour_db) {
  return new Promise((rs, rj) => {
    var sql = 'select * from sify_darc_modules_query  order by sequence asc ';
    sour_con.query(sql, function (err, result, fields) {
      if (err || !result.length > 0) {
        rj(new Error('No data available for archival. '));
        log.info('No data available for archival.');
        return false;
      }
      else {
        if (result.length > 0) rs(result);
      }
    });
  });
}
}