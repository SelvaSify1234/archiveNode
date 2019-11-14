var fs = require('fs');
var dateFormat = require('dateformat');

 var log;


module.exports = {
    info: function(message) {
        log.info(message);
        return true;
    },
    error: function(message) {
        log.error(message);
        return true;
    },

   create_log: function create_log()
{
    var name =dateFormat(new Date(), "yyyy-MM-dd"); 
    var file_name = 'logs/dbarchive_'+name+'.log';
    fs.open(file_name, 'w', function (err, file) {
      if (err) throw err;
      log.info(file_name+' Log file has created !');
    });
    
    //create a logger file
    console.log('create a logger file');
    log = require('simple-node-logger').createSimpleLogger(file_name);
    
    log.info('subscription to ', 'channel', ' accepted at .. ', new Date().toJSON());
    return true;   
  }

};

  