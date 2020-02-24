var fs = require('fs');
const bodyParser = require("body-parser");
var mysql = require('mysql');
var helmet = require("helmet");
var cors = require('cors');
const express = require('express');
var archive = require('./job_forumnxt');
var log = require('./log');
var forEach = require('async-foreach')
  .forEach;
var common = require('./common');
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
let dest_con;
let sour_con;
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
app.use(bodyParser.urlencoded({
  extended: true
}));

archive_job();
async function archive_job() {
  log.create_log();
  sour_con = mysql.createPool({
    host: '127.0.0.1'
    , user: 'root'
    , password: 'support2019'
    , database: 'suns0018'
    , connectionLimit: 15
    , acquireTimeout: 1000000
  });
  dest_con = mysql.createPool({
    host: '127.0.0.1'
    , user: 'root'
    , password: 'support2019'
    , database: 'world'
    , connectionLimit: 15
    , acquireTimeout: 1000000
  });
  customer_type = '1';
  sour_host = '127.0.0.1';
  dest_host = '127.0.0.1';
  sour_db = 'suns0018';
  dest_db = 'world';
  sour_port = '3306';
  dest_port = '3306';
  create_table = true;
  module_name = 'All';


  if (customer_type == '1') {
    await get_sales_data(sour_con, module_name, sour_db)
      .then(result => {
        if (result.length > 0) {
          msg = '';
          log.info(` ${result.length}  : Modules selected for archival process. `);
          var list = [];
          list.length = 0;
          forEach(result, async function (item, index, arr) {
            sel_query = item['sel_query_template'];
            del_query = item['del_query_template'];
            sequence = item['vt_tabid'];
            var str = sel_query.match(/WHERE\b/);
            if (str != null) {
              var query = sel_query.slice(0, str.index + 5);
              sour_table = query.match(new RegExp('FROM' + "(.*)" + 'WHERE'))[1];
              sour_table = sour_table.replace(/\s/g, "");
              dest_table = sour_table + '_archival_2';
            }
            await archive.do_archive(sour_con, dest_con, create_table, item['module_name'], sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query, del_query, item['vt_tabid'], item['id'])
              .then(stat => {
                if (stat) {
                  log.info(' Row No :' + index + ' Data Archive has been done.');
                  list.push(index);
                  if (result.length == list.length && result.length == (index + 1) || result.length == list.length) {
                    delete_all(sour_con, result, sour_db);
                    list.length = 0;
                  }
                } else {
                  log.info(' Row No :' + index + '  ' + ' Some reasone data archival has been NOT Done.');
                  list.push(index);
                  if (result.length == list.length && result.length == (index + 1) || result.length == list.length) {
                    delete_all(sour_con, result, sour_db);
                    list.length = 0;
                  }
                }
              })
              .catch(err => {
                log.error('\n----------------------\n');
                log.error(' Row No :' + index + '  ' + err.message + ' Module Name :' + item['module_name'] + ' vt_tabid ' + item['vt_tabid'] + 'Id :' + item['id']);
                log.error('\n----------------------\n');
                list.push(index);
                if (result.length == list.length && result.length == (index + 1) || result.length == list.length && result.length) {
                  delete_all(sour_con, result, sour_db);
                  list.length = 0;
                }
              });
          });
        }
      })
      .catch(err => {
        log.error('\n----------------------\n');
        log.error(err.message);
        log.error('\n----------------------\n');
      });
  }

  // async function source_conn(sour_pol, dest_pol) {
  //   return new Promise((rs, rj) => {
  //     sour_pol.getConnection(function (err, result) {
  //       if (err != null) {
  //         rj(new Error('Source database not connected !.. Please make sure source database configuration is valid.'));
  //         return false;
  //       }
  //       else {
  //         sour_con = result;
  //         log.info('Source database connected successfully.');
  //         dest_pol.getConnection(function (err,resl) {
  //           if (err != null) {
  //             rj(new Error('Destination database not connected !.. Please make sure source database configuration is valid.'));
  //             return false;
  //           }
  //           else {
  //             dest_con = resl;
  //             log.info('Destination database connected successfully.');
  //             rs(true);
  //           }
  //         });
  //       }
  //     });
  //   });
  // }
  function get_sales_data(sour_con, name, sour_db) {
    return new Promise((rs, rj) => {
      var sql = 'select * from sify_darc_modules_query order by sequence asc;';
      sour_con.query(sql, function (err, result, fields) {
        if (err || !result.length > 0) {
          rj(new Error('No data available for archival. '));
          log.info('No data available for archival.');
        } else {
          if (result.length > 0) rs(result);
        }
      });
    });
  }

  async function delete_all(sour_con, result, sour_db) {
    forEach(result, async function (item, index, arr) {
      await common.delete_data(sour_con, item['module_name'], item['del_query_template'], item['id'], sour_db)
        .then(rslt => {
          log.info(' Row No :' + index + ' Id :' + item['id'] + ' Data Archive has been done. Record Deleted');
        })
        .catch(err => {
          log.info(' Row No :' + index + ' Id :' + item['id'] + err.message);
        });
    });
    return;
  }
}
