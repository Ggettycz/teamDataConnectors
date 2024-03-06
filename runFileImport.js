const {mysql,DBLink} = require('./mysql_creds.js');
const {Connector} = require ('./elements/Connector.js')
const logMessage = require('../src/utils.js/logs.js');
global.specificLogName = 'runExport';


const connectorTypes = 'FileImport';

(async function(){
    // simple select so no transaction needed.
    const link = new DBLink();

    let query  = "SELECT UUID as uuid, Name as connectorName, ConnectorType as connectorType,"
        + " Configuration as configuration FROM Connectors"
        + " WHERE Active=1 AND NextRun < NOW()";
    if (connectorTypes != '') {
        query += " AND ConnectorType in ( ? )";
    }

    let rows = await link.query(query,[connectorTypes]);	//Do qry
    let dry_run = false;

    for (const row of rows) {
        const {uuid, connectorType, connectorName, configuration} = row;
        logMessage('--------- start of new Connector for ' + uuid, 'INFO');
        // run connector based on the type
        if (connectorType == 'FileImport') {

            let configuration = {
                'userId':2,
                'importPath': '/volume1/web/weatherstattion/data/',
                'filePrefix': 'garni_',
                'fileSuffix': 'json',
                'processedDir' : 'processed/',
                'errorDir' : 'error/',
                'recordSplit': 'NEW_LINE',
            };
            try{
                const connector = new Connector(link, uuid, connectorName, configuration);
                //connector.helper('dbStatsNotActiveStatsCreated'); break;
                await link.begin();

                if (! await connector.verifyConfiguration()) { continue; }
                logMessage('configuration verified', 'DEBUG');
                // get data to be exported ... 
                connector.helper('getParamIDsCount');
                if(dry_run){
                    logMessage("Dry-run enabled; rolling back DB", 'WARN');
                    await link.rollback();
                }else await link.complete();

        
            }catch(e){
                logMessage('Error: ' + e, 'ERROR');
            }
        }
    }
    // update Next Run if successful

    link.end();
   
})();


