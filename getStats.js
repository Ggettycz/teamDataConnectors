const {mysql,DBLink} = require('./mysql_creds.js');
const logMessage = require('../src/utils.js/logs.js');
const {Connector} = require('./elements/Connector.js');
global.specificLogName = 'runExport';


const connectorTypes = 'DataExport';
const STATS_RECORD = 64; // value for DataRecordStatus generated by StatsCreate. why 64? why not 

(async function(){
    // simple select so no transaction needed.
    const link = new DBLink();
    const uuid = 'f30c60de-cff2-11ee-9d77-9009d01d7a9f';
    let configuration = {
        'paramIdsList': [1,2,3,9,10,12]
    }


    const connector = new Connector(link, uuid, 'connectorName', configuration);
    await connector.verifyConfiguration();
    await connector.getStats();


    link.end();
   
})();

