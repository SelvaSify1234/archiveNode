// var http = require('http');
// request.post('http://localhost:4200/#/archival/add', {
//   json: {}
// }, (error, res, body) => {
//   if (error) {
//     console.error(error)
//     return
//   }
//   console.log(`statusCode: ${res.statusCode}`)
//   console.log(body)
// })


// include node fs module
var fs = require('fs');
var dateFormat = require('dateformat');

var name =dateFormat(new Date(), "yyyy-MM-dd"); 
console.log(name+'.log');
//console.log(dateFormat(new Date(), 'isoDateTime'));

//console.log(dateFormat(new Date(), "yyyy-MM-dd"+".log"));
var filename = 'logs/dbarchive_'+name+'.log';
 
fs.open(filename, 'w', function (err, file) {
  if (err) throw err;
  console.log('Saved!');
});

