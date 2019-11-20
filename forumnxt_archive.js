var log = require('./log');
var insert_columns;
var tbl_columns;

async function do_archive(sour_con, dest_con, create_table, module_name, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query, del_query, sequence) {
    return new Promise((rs, rj) => {
        return table_exists(dest_con, dest_db, dest_table)
            .then(stat => {
                if (stat) {
                    return import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query)
                        .then(rslt => {
                            if (rslt) {
                                rs(delete_data(sour_con, del_query, sequence, module_name, sour_db));
                            } else {
                                rj(new Error(`No data available for selected Select Module Name...`));
                                return false;
                            }
                        })
                } else if (create_table) {
                    return get_table_schema(sour_con, sour_table, dest_table)
                        .then(schema => {
                            return import_table_schema(dest_con, schema)
                                .then(stat => {
                                    var p$;
                                    if (stat) {
                                        return import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query)
                                            .then(rslt => {
                                                var p$;
                                                if (rslt) {
                                                    rs(delete_data(sour_con, del_query, sequence, module_name, sour_db));
                                                } else {
                                                    rj(new Error(`No data available for selected Select Module Name...`));
                                                    return false;
                                                }
                                            })
                                    }
                                });
                        });
                } else {
                    log.error('Table dose not exists in destination database . Also create table not enabled.');
                    // p$=Promise.reject(new Error('Table dose not exists in destination database . Also create table not enabled.'));
                    rj(new Error('Table dose not exists in destination database . Also create table not enabled.'));
                    return false;
                }
            });
        // }
        // else{
        //      log.error('Table dose not exists in destination database . Also create table not enabled.');
        //     p$=Promise.reject(new Error('Table dose not exists in destination database . Also create table not enabled.'));
        //     return false;
        // }
        // });
    });
}



async function table_exists(conn, dest_db, name) {
    var sql = `SELECT count(*) FROM information_schema.tables WHERE table_schema = '${dest_db}' AND table_name = '${name}'`;
    return new Promise((rs, rj) => {
        conn.query(sql, function(err, results, fields) {
            if (err) {
                rj(new Error('Error while getting info about table.'));
                return false;
            }
            if (results[0]['count(*)'] == 0) {
                rs(false);
            } else {
                rs(true);
            }
        });
    });
}
async function get_table_schema(conn, name, dest_table) {
    return new Promise((rs, rj) => {
        conn.query(`SHOW CREATE TABLE ${name}`, function(err, results, fields) {
            if (err) {
                rj(new Error(`Error while getting the "${name}" schema.`));
                return false;
            }
            var schema = (results.length > 0 && results[0] && results[0]['Create Table']) || null;
            if (!schema) {
                rj(new Error(`Error while getting the "${name}" schema.`));
                return false;
            }
            rs(schema = schema.replace(name, dest_table));
        });
    });
}

async function get_table_columns(conn, sour_db, sour_table) {
    return new Promise((rs, rj) => {
        var sql = 'SELECT COLUMN_NAME  FROM information_schema.columns WHERE table_name ="' + sour_table + '" AND table_schema ="' + sour_db + '"   ORDER BY ORDINAL_POSITION';
        conn.query(sql, function(err, rslt, fields) {
            if (err || !rslt) {
                rj(new Error(`Error while get columns names from source table...` + err.message));
                return false;
            } else {

                tbl_columns = rslt;
                for (var i = 0; i < rslt.length; i++) {
                    if (i < rslt.length - 1) {
                        if (i == 0)
                            insert_columns = rslt[i]['COLUMN_NAME'] + ',';
                        else
                            insert_columns += rslt[i]['COLUMN_NAME'] + ',';
                    } else
                        insert_columns += rslt[i]['COLUMN_NAME'];
                }
                rs(true);
            }
        });
    });
}


async function import_table_schema(conn, schema) {
    return new Promise((rs, rj) => {
        conn.query(schema, function(err, results, fields) {
            if (err || !results) {
                if (err.message.indexOf('ER_TABLE_EXISTS_ERROR:') >= 0) {
                    rs(true);
                } else {
                    rj(new Error(`Error while creating new table. ` + err.message));
                    return false;
                }
            } else {
                rs(true);
            }
        });
    });
}


function import_table_data(sour_con, dest_con, sour_db, dest_db, sour_host, dest_host, sour_port, dest_port, sour_table, dest_table, sel_query) {
    /*  Source and destination host and port is different */
    if (sour_host != dest_host && sour_port != dest_port) {
        return new Promise((rs, rject) => {          
            sour_con.query(sel_query, function(err, result, fields) {
                if (err || !result) {
                    rject(new Error(`Error while get data from source table..`));
                    return false;
                } else {
                    return get_table_columns(sour_con, sour_db, sour_table)
                        .then(rslt => {
                            if (rslt) {
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
                                    dest_con.query(sql, [values], function(err, result) {
                                        if (err || !result) {
                                            rject(new Error(`Error while import data to destination table.` + err.message));
                                            return false;
                                        } else {
                                            log.info('Number of records inserted: ' + result.affectedRows);
                                            rs(true);
                                        }
                                    });
                                } else {
                                    rject(new Error(`No data available for selected Select Module Name...`));
                                    return false;
                                }
                            }
                        });
                }
            });
        });
    }
    /* IF Source and Destination host and port are same.*/
    else {
        return new Promise((rs, rj) => {
            var tbl = sour_db + "." + sour_table;
            var query = replaceAll(sel_query, sour_table, tbl);
            //  var query= replaceAll(query,";","");
            var sql = "INSERT INTO " + dest_db + "." + dest_table + " (" + query + ") ";
             console.log(sql);
            dest_con.query(sql, function(err, result, fields) {
                if (err || !result) {
                    rj(new Error(`Error while get data from source table..`+ err.message));
                    return false;
                } else {
                    rs(true);
                }

            });
        });
    }
}


function delete_data(conn, del_query, sequence, module_name, sour_db) {
    return new Promise((rs, rj) => {
            conn.query(del_query, function(err, results, fields) {
                if (err || !results) {
                    rj(new Error(`Error while delete table data.`));
                    return false;
                } else {
                    log.info('Number of records deleted: ' + results.affectedRows);
                    log.log_entry(conn, sequence, module_name, '1', sour_db);
                    rs(true);
                }
            });
        })
        .then(sat => {
            return sat;
        });
}

function replaceAll(originalString, find, replace) {
    return originalString.replace(new RegExp(find, 'g'), replace);
}


module.exports = {
    do_archive , table_exists
}
