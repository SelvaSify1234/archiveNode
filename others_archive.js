var common = require('./common');
var log = require('./log');
var sel_query;
var insert_columns;
var tbl_columns;
global.sel_duration = '';

async function do_archive(sour_con, dest_con, create_table, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, rela_tables, condition) {
  return new Promise((resolve, reject) => {
    /* Check Source table Check*/
    common.table_exists(sour_con, sour_db, sour_table)
      .then(stat => {
        if (stat) {
          /* Check Destination table Check*/
          common.table_exists(dest_con, dest_db, dest_table)
            .then(dest_rst => {
              if (dest_rst) {
                if (rela_tables != null && rela_tables != '') sql = `Select * from ${sour_table}, ${rela_tables} where ${condition}`;
                else sql = `Select * from ${sour_db}.${sour_table} where ${condition}`;
                import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sql)
                  .then(rst => {
                    if (rst) {
                      if (rela_tables != null && rela_tables != '') sql = ` Delete ${sour_db}.${sour_table}  from ${sour_db}.${sour_table} ,  ${rela_tables} where ${condition}`
                      else sql = ` Delete from ${sour_db}.${sour_table} where ${condition}`;
                      common.delete_data(sour_con, sql, 0, sour_table, sour_db, sel_duration)
                        .then(rsl => {
                          if (!rsl) {
                            reject(new Error(`Error while delete source table data. Err Msg : ` + err.message));
                            return false;
                          }
                          else resolve(true);
                        })
                        .catch(err => {
                          reject(new Error(`Error while delete source table data. Err Msg : ` + err.message));
                          return false;
                        });
                    }
                    else {
                      reject(new Error(`No data available for archival.`));
                      return false;
                    }
                  })
                  .catch(err => {
                    log.error(err.message);
                    reject(new Error(err.message));
                    return false;
                  });
              }
              else if (create_table) {
                common.get_table_schema(sour_con, sour_table, dest_table)
                  .then(schema => {
                    common.import_table_schema(dest_con, schema)
                      .then(stat => {
                        if (stat) {
                          if (rela_tables != null && rela_tables != '') sql = `Select * from ${sour_table}, ${rela_tables} where ${condition}`;
                          else sql = `Select * from ${sour_db}.${sour_table} where ${condition}`;
                          import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sql)
                            .then(rst => {
                              if (rst) {
                                if (rela_tables != null && rela_tables != '') sql = ` Delete ${sour_db}.${sour_table}  from ${sour_db}.${sour_table} ,  ${rela_tables} where ${condition}`
                                else sql = ` Delete from ${sour_db}.${sour_table} where ${condition}`;
                                common.delete_data(sour_con, sql, 0, sour_table, sour_db, sel_duration)
                                  .then(rsl => {
                                    if (!rsl) {
                                      reject(new Error(`Error while delete source table data. Err Msg : ` + err.message));
                                      return false;
                                    }
                                    else resolve(true);
                                  })
                                  .catch(err => {
                                    reject(new Error(`Error while delete source table data. Err Msg : ` + err.message));
                                    return false;
                                  });
                              }
                              else {
                                reject(new Error(`No data available for archival.`));
                                return false;
                              }
                            })
                            .catch(err => {
                              log.error(err.message);
                              reject(new Error(err.message));
                              return false;
                            });
                        }
                      });
                  });
              }
              else {
                reject(new Error('Table dose not exists in destination database . Also create table not enabled.'));
                return false;
              }
            });
        }
        else {
          reject(new Error('Table dose not exists in source database. Make sure table name is valid .'));
          return false;
        }
      });
  });
}

function import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query) {
  /* If Source & Destination host and port is different */
  if (sour_host == dest_host && sour_port == dest_port) {
    return new Promise((resolve, reject) => {
      var pre_query = new Date()
        .getTime();
      sour_con.query(sel_query, function (err, result, fields) {
        /* Query Execution Time Check */
        var post_query = new Date()
          .getTime();
        sel_duration = '';
        sel_duration = (post_query - pre_query) / 1000;
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
                      log.info('Number of records inserted: ' + result.affectedRows);
                      resolve(true);
                    }
                  });
                }
                else {
                  reject(new Error(`No data available for archival.`));
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

module.exports = { do_archive }
