var log = require('./log');
var common = require('./common');
var datetime = require('node-datetime');
var insert_columns;
var tbl_columns;
global.sel_duration = '';
global.aff_row='';
var ins_st_time;
var ins_end_time;

async function do_archive(sour_con, dest_con, create_table, module_name, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query, del_query, sequence) {
  return new Promise((resolve, reject) => {
    return common.table_exists(dest_con, dest_db, dest_table)
      .then(stat => {
        if (stat) {
          return import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query)
            .then(result => {
              if (result) {
                resolve(common.delete_data(sour_con, del_query, sequence, module_name, sour_db, sel_duration,ins_st_time,ins_end_time,sel_query));
              }
              else {
                reject(new Error(`Selected Module name do not have data to archival.`));
                return false;
              }
            })
            .catch(err => {
              //log.error(err.message);
              log.log_entry(sour_con, sequence, module_name, '2', sour_db, sel_duration, 0,aff_row, err.message.replace(/[^\w\s]/gi, ''),ins_st_time,ins_end_time,'0000-00-00 00:00:00','0000-00-00 00:00:00',sel_query,del_query);
              reject(new Error(err.message));
              return false;
            });
        }
        else if (create_table) {
          return common.get_table_schema(sour_con, sour_table, dest_table)
            .then(schema => {
              return common.import_table_schema(dest_con, schema)
                .then(stat => {
                  if (stat) {
                    return import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query)
                      .then(rst => {
                        if (rst) {
                          resolve(common.delete_data(sour_con, del_query, sequence, module_name, sour_db, sel_duration,ins_st_time,ins_end_time,sel_query));
                        }
                        else {
                          reject(new Error(`Selected Module name do not have data to archival.`));
                          return false;
                        }
                      })
                      .catch(err => {
                        //log.error(err.message);
                         log.log_entry(sour_con, sequence, module_name, '2', sour_db, sel_duration, 0,aff_row, err.message.replace(/[^\w\s]/gi, ''),ins_st_time,ins_end_time,'0000-00-00 00:00:00','0000-00-00 00:00:00',sel_query,del_query);
                        reject(new Error(err.message));
                        return false;
                      });
                  }
                })
                .catch(err => {
                 // log.error(err.message);
                 log.log_entry(sour_con, sequence, module_name, '2', sour_db, 0, 0,0, err.message.replace(/[^\w\s]/gi, ''),ins_st_time,ins_end_time,'0000-00-00 00:00:00','0000-00-00 00:00:00',sel_query,del_query);
                  reject(new Error(err.message));
                  return false;
                });
            })
            .catch(err => {
             // log.error(err.message);
             log.log_entry(sour_con, sequence, module_name, '2', sour_db, 0, 0,0, err.message.replace(/[^\w\s]/gi, ''),null,null,null,null,sel_query,del_query);
              reject(new Error(err.message));
              return false;
            });
        }
        else {
          log.error('Table dose not exists in destination database . Also create table not enabled.');
          reject(new Error('Table dose not exists in destination database . Also create table not enabled.'));
          return false;
        }
      });
  });
}
async function get_table_columns(conn, sour_db, sour_table) {
  return new Promise((resolve, reject) => {
    var sql = 'SELECT COLUMN_NAME  FROM information_schema.columns WHERE table_name ="' + sour_table + '" AND table_schema ="' + sour_db + '"   ORDER BY ORDINAL_POSITION';
    conn.query(sql, function (err, result, fields) {
      if (err || !result) {
        reject(new Error(`Error while get columns names from source table...` + err.message));
        return false;
      }
      else {
        tbl_columns = result;
        for (var i = 0; i < result.length; i++) {
          if (i < result.length - 1) {
            if (i == 0) insert_columns = result[i]['COLUMN_NAME'] + ',';
            else insert_columns += result[i]['COLUMN_NAME'] + ',';
          }
          else insert_columns += result[i]['COLUMN_NAME'];
        }
        resolve(true);
      }
    });
  });
}

function import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query) {
  /* If Source & Destination host and port is different */
  if (sour_host == dest_host && sour_port == dest_port) {
    return new Promise((resolve, reject) => {
      var pre_query = new Date().getTime();
      var dt = datetime.create();
      ins_st_time = dt.format('Y-m-d:H:M:S');   
      sour_con.query(sel_query, function (err, result, fields) {
        /* Query Execution Time Check */
        var post_query = new Date().getTime();
        aff_row='';
        sel_duration = '';                    
        sel_duration = (post_query - pre_query) / 1000;
        var dt_end = datetime.create();
        ins_end_time = dt_end.format('Y-m-d:H:M:S');  
        if (err || !result) {
          reject(new Error(`Error while get data from source table. Err Msg : ` + err.message));
          return false;
        }
        else {
          return get_table_columns(sour_con, sour_db, sour_table)
            .then(rst => {
              if (rst) {
                if (result && result.length > 0) {
                  var sql = "INSERT INTO " + dest_table + " (" + insert_columns + ") VALUES ?";
                  var values = [];
                  for (var i = 0; i < result.length; i++) {
                    var temp_values = [];
                    for (var j = 0; j < tbl_columns.length; j++) {
                      temp_values.push(result[i][tbl_columns[j]['COLUMN_NAME']]);
                    }
                    values.push(temp_values);
                  }
                  dest_con.query(sql, [values], function (err, result) {
                    if (err || !result) {
                      reject(new Error(`Error while import data to destination table. Err Msg: ` + err.message));
                      return false;
                    }
                    else {
                      aff_row=result.affectedRows;
                      log.info(`Number of records inserted:  ${aff_row}`);
                      resolve(true);
                    }
                  });
                }
                else {
                  reject(new Error(`Selected Module name do not have data to archival.`));
                  return false;
                }
              }
            });
        }
      });
    });
  }
  /* IF Source & Destination host and port are same.*/
  else {
    return new Promise((resolve, reject) => {
      var tbl = sour_db + "." + sour_table;
      var query = common.replace_all(sel_query, sour_table, tbl);
      var sql = "INSERT INTO " + dest_db + "." + dest_table + " (" + query + ") ";
      dest_con.query(sql, function (err, result, fields) {
        if (err) {
          reject(new Error(`Error while import data to destination table. Err Msg : ` + err.message));
        }
        else {
          resolve(true);
        }
      });
    });
  }
}
module.exports = { do_archive, import_table_data, get_table_columns }
