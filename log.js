var fs = require('fs');
var dateFormat = require('dateformat');
var datetime = require('node-datetime');
var log;


module.exports = {
    info: function (message) {
        log.info(message);
        return true;
    }
    , error: function (message) {
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

    log_entry: function log_entry(conn, sequence, module_name, status, sour_db,sel_duration,del_duration) {
        return new Promise((rs, rj) => {
            var dt = datetime.create();
            var formatted = dt.format('Y-m-d:H:M:S');

            var sql = `insert into ${sour_db}.archival_status_log (vt_tabid,module_name,status,process_date,sel_duration,del_duration) values(${sequence},'${module_name}',${status},'${formatted}','${sel_duration}','${del_duration}')`;

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