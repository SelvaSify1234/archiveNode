var log = require('./log');
var datetime = require('node-datetime');
var del_duration;

async function table_exists(conn, dest_db, name) {
  var sql = `SELECT count(*) FROM information_schema.tables WHERE table_schema = '${dest_db}' AND table_name = '${name}'`;
  return await new Promise((resolve, reject) => {
    conn.query(sql, function (err, results, fields) {
      if (err) {
        reject(new Error('Error while checking table exists or not. Err Msg :' + err.message+', Err Code: '+err.code));
        return false;
      }
      if (results[0]['count(*)'] == 0) {
        resolve(false);
      }
      else {
        resolve(true);
      }
    });
  });
}

async function get_table_schema(conn, name, dest_table) {
  return new Promise((resolve, reject) => {
    conn.query(`SHOW CREATE TABLE ${name}`, function (err, results, fields) {
      if (err) {
        reject(new Error(`Error while getting the "${name}" schema.. Err Msg :` + err.message+', Err Code: '+err.code));
        return false;
      }
      var schema = (results.length > 0 && results[0] && results[0]['Create Table']) || null;
      if (!schema) {
        reject(new Error(`Error while getting the "${name}" schema.. Err Msg :` + err.message+', Err Code : '+err.code));
        return false;
      }
      else {
        if (schema.indexOf('CONSTRAINT') != -1 || schema.indexOf('constraint') != -1) {
          var foreign_key = schema.match(new RegExp('CONSTRAINT' + "(.*)" + 'FOREIGN KEY'));
          for_key = foreign_key[1].replace(/\s/g, "");
          for_key = for_key.replace(/`/gi, "");
          /*For Others Module*/
          //schema = schema.replace(for_key, for_key+'_archival');         
          // schema = schema.replace(for_key, dest_table + 'Id' + '_archival');
          // schema = schema.replace(name, dest_table);
          schema = replace_all(schema, name, dest_table);
          resolve(schema);
        }
        else {
          resolve(schema = schema.replace(name, dest_table));
        }
      }
    });
  });
}
async function import_table_schema(conn, schema) {
  return await new Promise((resolve, reject) => {
    conn.query(schema, function (err, results, fields) {
      if (err || !results) {
        if (err.message.indexOf('ER_TABLE_EXISTS_ERROR:') >= 0) {
          resolve(true);
        }
        else {
          reject(new Error('Error while creating new table no destination database. Err Msg : ' + err.message  +', Err Code: '+err.code));
         // return false;
        }
      }
      else {
        resolve(true);
      }
    });
  });
}


async function delete_data(conn, module_name,del_query,id,sour_db) {
  return await new Promise((resolve, reject) => {
  let sql = `select * from archival_status_log where id='${id}' and status='${1}' order by process_date desc LIMIT 1 ;`

  conn.query(sql, function (error, resl, fields) {
   if(error || resl.length == 0)
     { 
      if(error != null) 
      reject(new Error(`Error while delete source table data. Err Msg : `  + error.message +', Err Code: '+err.code  ));
     else 
      reject(new Error(`Insert Not completed, So Delete  Action Not Performed.. ` ));
    }
   else{

  // console.log('Res ===>',resl);
    var pre_query = new Date().getTime();
    var dt = datetime.create();
     var del_st_time = dt.format('Y-m-d:H:M:S');
    conn.query(del_query,60000, function (err, results, fields) {
      /* Query Execution Time Check */
      var post_query = new Date().getTime();
      del_duration = (post_query - pre_query) / 1000;
    var dt_end = datetime.create();
     var del_end_time = dt_end.format('Y-m-d:H:M:S');
      if (err || !results ) {
        console.log(err.message);
        var msg = `Error while delete source table data. Err Msg : `  + err.message  +', Err Code: '+err.code;
       // log.log_update(conn, sequence, module_name, '2', sour_db, sel_duration, del_duration, ins_row,0, msg.replace(/[^\w\s]/gi, ''), st_time, end_time, del_st_time, del_end_time, sel_query, del_query);
         log.log_update(conn, module_name, '2',sour_db, del_duration, 0, msg.replace(/[^\w\s]/gi, ''),  del_st_time, del_end_time, id);
        reject(new Error(`Error while delete source table data. Err Msg : `  + err.message +', Err Code: '+err.code ));
        //return false;
      }
      else {
        log.info('Number of records deleted: ' + results.affectedRows);
        var msg = 'Data Archive has been done';
        log.log_update(conn, module_name, '1',sour_db, del_duration, results.affectedRows, msg.replace(/[^\w\s]/gi, ''),  del_st_time, del_end_time, id);
        resolve(true);
      }
    });
    }
  });
  });
}

// async function delete_data(conn, del_query, sequence, module_name, sour_db, sel_duration, st_time, end_time, sel_query, ins_row) {
//   return await new Promise((resolve, reject) => {
//     var pre_query = new Date().getTime();
//     var dt = datetime.create();
//      var del_st_time = dt.format('Y-m-d:H:M:S');
//     conn.query(del_query, function (err, results, fields) {
//       /* Query Execution Time Check */
//       var post_query = new Date().getTime();
//       del_duration = (post_query - pre_query) / 1000;
//     var dt_end = datetime.create();
//      var del_end_time = dt_end.format('Y-m-d:H:M:S');
//       if (err || !results) {
//         var msg = `Error while delete source table data. Err Msg : `  + err.message  + err.message+', Err Code: '+err.code;
//         log.log_entry(conn, sequence, module_name, '2', sour_db, sel_duration, del_duration, ins_row,0, msg.replace(/[^\w\s]/gi, ''), st_time, end_time, del_st_time, del_end_time, sel_query, del_query);
//         reject(new Error(`Error while delete source table data. Err Msg : `  + err.message  + err.message+', Err Code: '+err.code ));
//         return false;
//       }
//       else {
//         log.info('Number of records deleted: ' + results.affectedRows);
//         var msg = 'Data Archive has been done';
//         log.log_entry(conn, sequence, module_name, '1', sour_db, sel_duration, del_duration,ins_row, results.affectedRows, msg, st_time, end_time, del_st_time, del_end_time, sel_query, del_query);
//         resolve(true);
//       }
//     });
//   });
// }

// async function delete_data(conn, del_query, sequence, module_name, sour_db, sel_duration, st_time, end_time, sel_query, ins_row) {
//   return await new Promise((resolve, reject) => {
//     var pre_query = new Date().getTime();
//     var dt = datetime.create();
//      var del_st_time = dt.format('Y-m-d:H:M:S');

// conn.getConnection(function(err, con) {

//      con.beginTransaction(function(error) {
//       if(error)
//       {
//               con.rollback(function() {
//                         con.release();
//                         //Failure
//                     });
//       }
//       else{
//    con.query(del_query, function (err, results, fields) {
//       /* Query Execution Time Check */
//       var post_query = new Date().getTime();
//       del_duration = (post_query - pre_query) / 1000;
//     var dt_end = datetime.create();
//      var del_end_time = dt_end.format('Y-m-d:H:M:S');
//       if (err || !results) {
//         con.rollback(function() {
//                         con.release();
//                         //Failure
//                     });
//         var msg = `Error while delete source table data. Err Msg : `  + err.message  +', Err Code: '+err.code;
//         log.log_entry(conn, sequence, module_name, '2', sour_db, sel_duration, del_duration, ins_row,0, msg.replace(/[^\w\s]/gi, ''), st_time, end_time, del_st_time, del_end_time, sel_query, del_query);
//         reject(new Error(`Error while delete source table data. Err Msg : `  + err.message  +', Err Code: '+err.code ));
//         return false;
//       }
//       else {

// con.commit(function(err) {
//                         if (err) {
//                             con.rollback(function() {
//                                 con.release();
//                                 //Failure
//                             });
//                         } else {
//                             con.release();
//                             //Success
//                         }
//                       });

//         log.info('Number of records deleted: ' + results.affectedRows);
//         var msg = 'Data Archive has been done';
//         log.log_entry(conn, sequence, module_name, '1', sour_db, sel_duration, del_duration,ins_row, results.affectedRows, msg, st_time, end_time, del_st_time, del_end_time, sel_query, del_query);
//         resolve(true);
//       }
//     });
//       }
//      });
//    });
    
//   });
// }

function replace_all(originalString, find, replace) {
  return originalString.replace(new RegExp(find, 'g'), replace);
}
module.exports = {
  table_exists
  , get_table_schema
  , import_table_schema
  , delete_data
  , replace_all
}
