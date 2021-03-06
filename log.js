var fs = require('fs');
var dateFormat = require('dateformat');
var datetime = require('node-datetime');
var log;

module.exports = {
   info: function (message) {
      log.info(message);
      return true;
   },
    error: function (message) {
      log.error(message);
      return true;
   }, 
   create_log: function create_log() {
      var dt = datetime.create();
      var name = dt.format('Y_m_d__H_M_S');
      var file_name = 'logs/dbarchive_' + name + '.log';
      fs.open(file_name, 'w', function (err, file) {
         if (err) throw err;
         log.info(file_name + ' Log file has created !');
      });
      //create a logger file
      log = require('simple-node-logger').createSimpleLogger(file_name);
      log.info('subscription to ', 'channel', ' accepted at .. ', new Date().toJSON());
      return true;
   }, 
    log_entry: function log_entry(conn, sequence, module_name, status, sour_db, sel_duration, del_duration,insert_rows, del_rows,err,st_time,end_time,pre_query,post_query,sel_query,del_query,id) {
      return new Promise((rs, rj) => {
         var dt = datetime.create();
         var formatted = dt.format('Y-m-d:H:M:S');
         sel_query = sel_query.replace(/'/gi, '');
         del_query = del_query.replace(/'/gi, '');
         var sql = `insert into ${sour_db}.archival_status_log (vt_tabid,module_name,status,process_date,insert_duration,del_duration,ins_rows,del_rows,err_msg,ins_st_time,ins_end_time,del_st_time,del_end_time,sel_query,del_query,id) values(${sequence},'${module_name}',${status},'${formatted}','${sel_duration}','${del_duration}','${insert_rows}','${del_rows}','${err}','${st_time}','${end_time}','${pre_query}','${post_query}','${sel_query}','${del_query}','${id}')`;
         conn.query(sql, function (err, result, fields) {
            if (err || !result) {
               console.log(err);
               rj(new Error('Log entry error ', 'some service ', +err.message));
             //  return false;
            }
            //return true;
         });
      });
   },

   log_update: function log_update(conn, module_name, status, sour_db,del_duration,del_rows,err,st_time,end_time,id) {
      return new Promise((rs, rj) => {
         var dt = datetime.create();
         var formatted = dt.format('Y-m-d:H:M:S');
         //sel_query = sel_query.replace(/'/gi, '');
        // del_query = del_query.replace(/'/gi, '');
         var sql = `update ${sour_db}.archival_status_log set status='${status}',del_duration='${del_duration}',del_rows='${del_rows}',err_msg='${err}',del_st_time='${st_time}',del_end_time='${end_time}' where id='${id}' and module_name='${module_name}' ;`;
            conn.query(sql, function (err, result, fields) {
            if (err || !result) {
               console.log(err);
               rj(new Error('Log entry error ', 'some service ', +err.message));
               return false;
            }
            return true;
         });
      });
   }


};
