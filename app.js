var fs = require('fs');
const bodyParser = require("body-parser");
var mysql = require('mysql');
var helmet = require("helmet");
var cors = require('cors');
const express = require('express');
var archive = require('./forumnxt_archive');
var archive_others = require('./others_archive');
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

/* Archive Data Post Call*/
app.post('/db/archive-manual', function (req, res, next) {
  log.create_log();
  dbkeys = Object.keys(req.body.database_config);
  for (var key in req.body['database_config'][dbkeys]) {
    eval(key + " = req.body['database_config'][dbkeys][key]");
  }
  sour_con = mysql.createConnection({
    host: mysql_source_host
    , user: mysql_source_username
    , password: mysql_source_password
    , database: mysql_source_database
  });
  dest_con = mysql.createConnection({
    host: mysql_destination_host
    , user: mysql_destination_username
    , password: mysql_destination_password
    , database: mysql_destination_database
  });
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
        /* Archive Data For Forumnxt Type */
        if (customer_type == '1' && sour_con.state == 'authenticated' && dest_con.state == 'authenticated') {
          get_sales_data(sour_con, module_name, sour_db)
            .then(result => {
              if (result.length > 0) {
                msg = '';
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
                  archive.do_archive(sour_con, dest_con, create_table, module_name, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query, del_query, item['vt_tabid'])
                    .then(stat => {
                      if (stat) {
                        log.info('Data Archive has been done.');
                        if (index == result.length - 1) {
                          res.setHeader('Content-Type', 'application/json');
                          res.send({ message: 'Data archival process has been completed.' });
                          res.end();
                        }
                      }
                      else {
                        log.info('Some reasone data archival has been NOT Done.');
                        if (index == result.length - 1) {
                          res.setHeader('Content-Type', 'application/json');
                          res.send({ message: 'Some reasone data archival has been NOT Done.' });
                          res.end();
                        }
                      }
                    })
                    .catch(err => {
                      log.error('\n----------------------\n');
                      msg += ' Row No :' + index + '  ' + err.message;
                      log.error(err.message);
                      log.error('\n----------------------\n');
                      log.log_entry(sour_con, item['vt_tabid'], module_name, '2', sour_db, 0, 0,0);
                      if (err.message != null && index == result.length - 1) {
                        res.setHeader('Content-Type', 'application/json');
                        res.send({ message: msg });
                        res.end();
                        msg = '';
                      }
                    });
                });
              }
            })
            .catch(err => {
              log.error('\n----------------------\n');
              log.error(err.message);
              log.error('\n----------------------\n');
              if (sequence != 0) log.log_entry(sour_con, item['vt_tabid'], module_name, '2', sour_db, 0, 0,0);
              res.setHeader('Content-Type', 'application/json');
              res.send({ message: err.message });
              res.end();
            });
        }
        else {
          /* Archive Data For Others Type */
          create_table = '';
          tblkeys = Object.keys(req.body['table_config'][dbkeys]);
          forEach(tblkeys, function (item, index, arr) {
            sour_table = req.body.table_config[dbkeys][item]['source_table'];
            dest_table = req.body.table_config[dbkeys][item]['destination_table'];
            rela_tables = req.body.table_config[dbkeys][item]['related_tables'];
            condition = req.body.table_config[dbkeys][item]['condition'];
            create_table = req.body.table_config[dbkeys][item]['create_destination_table_if_not_exists'];
            /* Do Archive */
            archive_others.do_archive(sour_con, dest_con, create_table, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, rela_tables, condition)
              .then(result => {
                if (result) {
                  log.info('Data Archive has been done.');
                  if (index == tblkeys.length - 1) {
                    res.setHeader('Content-Type', 'application/json');
                    res.send({ message: 'Data archival process has been completed.' });
                    res.end();
                  }
                }
                else {
                  log.info('Some reasone data archival has been NOT Done.');
                  if (index == tblkeys.length - 1) {
                    res.setHeader('Content-Type', 'application/json');
                    res.send({ message: 'Some reasone data archival has been NOT Done.' });
                    res.end();
                  }
                }
              })
              .catch(err => {
                log.error('\n----------------------\n');
                msg += ' Row No :' + index + '  ' + err.message;
                log.error(err.message);
                log.error('\n----------------------\n');
                log.log_entry(sour_con, 0, sour_table, '2', sour_db, 0, 0,0);
                if (err.message != null && index == tblkeys.length - 1) {
                  res.setHeader('Content-Type', 'application/json');
                  res.send({ message: msg });
                  res.end();
                  msg = '';
                }
              });
          });
        }
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
    var sql = 'select * from sify_darc_modules_query where module_name="' + name + '"  order by sequence asc';
    sour_con.query(sql, function (err, result, fields) {
      if (err || !result.length > 0) {
        rj(new Error('Module Name is not valid. Module Name data not exist. '));
        log.info('Module Name is not valid. Module Name data not exist');
        return false;
      }
      else {
        if (result.length > 0) rs(result);
      }
    });
  });
}

app.listen(3000, () => {});
module.exports = app;
