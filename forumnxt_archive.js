var log = require('./log');
var sel_query;
var del_query;
var sour_table;
var dest_table;
var insert_columns;

function do_archive(sour_con,dest_con,create_table,module_name,sour_db,dest_db,sour_host,dest_host,sour_port,dest_port)
{
    return get_sales_data(sour_con,module_name,sour_db).then(status =>{
       var p$;
        if(status){
            return table_exists(dest_con,dest_table)
            .then( stat => {
                var p$;
                if(stat){
                    return import_table_data(sour_con,dest_con,sour_db,dest_db,sour_host,dest_host,sour_port,dest_port)
                    .then(rslt=>{
                            var p$;
                            if(rslt)
                            {  return  delete_data(sour_con,del_query); }
                        }
                    )
                    //p$=Promise.resolve(true);
                }
        else if(create_table){
             return table_exists(dest_con,dest_table)
              .then( stat => {
                var p$;
                if(stat){
                    return import_table_data(sour_con,dest_con,sour_db,dest_db,sour_host,dest_host,sour_port,dest_port)
                    .then(rslt=>{
                            var p$;
                            if(rslt)
                            {  return  delete_data(sour_con,del_query); }
                        }
                     )
                    //p$=Promise.resolve(true);
                }
                else{
                    p$=get_table_schema(sour_con,sour_table,dest_table)
                    .then(schema => {
                        return import_table_schema(dest_con,schema)
                        .then( stat => {
                            var p$;
                            if(stat){
                                return import_table_data(sour_con,dest_con,sour_db,dest_db,sour_host,dest_host,sour_port,dest_port)
                                .then(rslt=>{
                                        var p$;
                                        if(rslt)
                                        {  return  delete_data(sour_con,del_query); }
                                    }
                                )
                                //p$=Promise.resolve(true);
                            }
                        });                        
                    });
                }
            });
          }
     else{
             log.error('Table doesnt exists in dest. Also create table NOT enabled.');
             p$=Promise.reject(new Error('Table doesnt exists in dest. Also create table NOT enabled.'));
         }
                p$=Promise.reject(new Error('Archive has been completed.'));
            });
        }
        else{
             log.error('Table doesnt exists in dest. Also create table NOT enabled.');
            p$=Promise.reject(new Error('Table doesnt exists in dest. Also create table NOT enabled.'));
        }
    });    
}

async function get_sales_data(sour_con,name,sour_db)
{
    return new Promise((rs,rj)=>{
   var sql  ='select * from sify_darc_modules_query where module_name="'+ name  +'"  order by sequence asc';
   sour_con.query(sql, function(err,result,fields){
   if(err || !result)
    { rj(new Error('Source Database Select Table error ','some service ',+ err.message));
      return false;
    }   
    else{
        if(result.length>0)
        { 
            for(var i=0; i < result.length; i++)
            {
                sel_query =  result[i]['sel_query'];
                del_query =  result[i]['del_query'];
        
                sour_table =   sel_query.match(new RegExp('FROM' + "(.*)" + 'WHERE'))[1];
               sour_table = sour_table.substring(0,sour_table.indexOf("WHERE"));
               sour_table = sour_table.replace(/\s/g, "");
                dest_table = sour_table;
               dest_table = dest_table+'_archival';
                //sel_query = sel_query.replace('/'+sour_table+'/g',  sour_db  +'.'+sour_table);
                //del_query = del_query.replace(/'"+sour_table+"'/g',  sour_db  +'.'+sour_table);
            }    
           rs(true);
        }
       else{
            rj(new Error('Module Name is not valide. Module Name data not exist. '));
            return false;
          }        
      }      
    });
  });
}


async function table_exists(conn,name)
{
    return new Promise((rs,rj)=>{
        conn.query('SHOW TABLES LIKE ?',[name],function(err,results,fields){
            if(err){
                rj(new Error('Error while getting info about table.'));
                return false;
            }
            if(results.length==0){ rs(false); }
            else{ rs(true);    }
        });
    });
}
     function get_table_schema(conn,name,dest_table)
     {
        return new Promise((rs,rj)=>{
            conn.query(`SHOW CREATE TABLE ${name}`,function(err,results,fields){
                if(err){
                    rj(new Error(`Error while getting the "${name}" schema.`));
                    return false;
                }
                var schema=(results.length > 0 && results[0] && results[0]['Create Table'])||null;
                if(!schema){
                    rj(new Error(`Error while getting the "${name}" schema.`));
                    return false;
                }
                rs( schema =schema.replace(name,dest_table));
            });
        });
    }

     function get_table_columns(conn,sour_table)
     {
        return new Promise((rs,rj)=>{
               var sql= 'SELECT COLUMN_NAME  FROM information_schema.columns WHERE table_name ="' +sour_table+ '" ';
            conn.query(sql,function(err,rslt,fields){
                if(err || !rslt){
                    rj(new Error(`Error while get columns names from source table...`+err.message));
                        return false  ;
                    }
                else{

                    for(var i=0; i< rslt.length;i++)
                    {
                     if(i < rslt.length-1)
                     { if(i==0)
                     insert_columns = rslt[i]['COLUMN_NAME']+',';
                     else
                      insert_columns += rslt[i]['COLUMN_NAME']+',';
                      }
                      else 
                     insert_columns += rslt[i]['COLUMN_NAME'];
                    }                   
                     rs(true);
                  }
            });
        });
    }
    

    function import_table_schema(conn,schema)
    {
        return new Promise((rs,rj)=>{
            conn.query(schema,function(err,results,fields){
                if(err || !results){
                    rj(new Error(`Error while creating new table. `+err.message));
                     return false ;
                }
                else{ rs(true);}
            });
        });
    }


  function import_table_data(sour_con,dest_con,sour_db,dest_db,sour_host,dest_host,sour_port,dest_port)
{ 
    /*  Source and destination host and port is different */
    if(sour_host != dest_host && sour_port != dest_port )
    {
     return new Promise((rs,rj)=>{
    sour_con.query(sel_query, function (err, result, fields) {
    if(err || !result){
        rj(new Error(`Error while get data from source table..`));
        return false;
    }
    else{
        return get_table_columns(sour_con,sour_table)
        .then(rslt=>{
         var p$;
         if(rslt)
         {  
        if(result && result.length>0) {
            var sql = "INSERT INTO " +dest_table+ " ("+insert_columns+") VALUES ?";
            var values = [];
            for (var i = 0; i < result.length; i++) {
                values.push([result[i].salesinvoiceid, result[i].cf_salesinvoice_sales_invoice_date,result[i].status]);
                }
      
             dest_con.query(sql, [values], function (err, result) {
              if(err || !result){
                  rj(new Error(`Error while import data to destination table.` +err.message));
                  return false;
                  }
              else{
                  log.info('Number of records inserted: ' + result.affectedRows);
                  rs(true);
                  }         
            });
        }
       else{        
                rj(new Error(`No data available for selected Select Module Name...` ));
                return false; }                 
           }
       });
      }
    });
   });
   }
    /* IF Source and Destination host and port are same.*/
 else{
         return new Promise((rs,rj)=>{
             var tbl=sour_db+"."+sour_table;
             var query= replaceAll(sel_query,sour_table,tbl);
             var query= replaceAll(query,";","");
        var sql = "INSERT INTO " +dest_db+"."+dest_table+ " ("+query+") ";
        dest_con.query(sql, function (err, result, fields) {
     if(err || !result){
        rj(new Error(`Error while get data from source table..`));
        return false;
        }
    else{      rs(true);     }
      
         });
      });
   }
}  


function delete_data(conn,del_query)
{
    return new Promise((rs,rj)=>{
        conn.query(del_query,function(err,results,fields){
            if(err || !results){
                rj(new Error(`Error while delete table data.`));
                return false;
              }
            else{
                log.info('Number of records deleted: ' + results.affectedRows);
                 p$=Promise.reject(new Error('Number of records deleted : ' + results.affectedRows));
                rs(true);
             }
        });
    });
}

function replaceAll(originalString, find, replace) 
{
  return originalString.replace(new RegExp(find, 'g'), replace);
}

  
module.exports= {
    do_archive,get_sales_data,table_exists
    }