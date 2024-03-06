... hotovo pro aktualni import asi nepouzivat dal
const {mysql,DBLink} = require('./mysql_creds.js');
const fs = require('fs');
const logMessage = require('../src/utils.js/logs.js');

const dry_run=false;


(async function(){
    // simple select so no transaction needed.
    const link = new DBLink();
    await link.begin();

    try{
        const path = '/volume1/web/weatherstation/data/garni_2024-02-';
        for (let i = 0; i <= 23; i++) {
            const fileName = `26-${i.toString().padStart(2, '0')}.json`;
            const lines = fs.readFileSync(path+fileName, 'utf-8').split('\n').filter(Boolean);
            logMessage(fileName);
            for (let line of lines) {
            //await processLine(line); // Process each line
                const data = JSON.parse(line);
                const sql1 = 'INSERT INTO DataRecords (DataTypeID, UserID, ControlPlaceID, RecordTime) VALUES (2,2,12, ?)';
            
                try {
                    const result = await link.query(sql1,data.dateutc);
                    let dataRecordId = result.insertId;
                    if (typeof dataRecordId === 'undefined') {
                        throw new Error('DataRecord insertion failed: No result returned');
                    }
                    const sql2 = 'INSERT INTO DataRecordNumberParameters (RecordID, ParameterTypeID, Value) VALUES '
                    + ` (${dataRecordId},1,${data.baromin}),`
                    + ` (${dataRecordId},2,${data.tempf}),`
                    + ` (${dataRecordId},3,${data.dewptf}),`
                    + ` (${dataRecordId},4,${data.humidity}),`
                    + ` (${dataRecordId},5,${data.windspeedmph}),`
                    + ` (${dataRecordId},6,${data.windgustmph}),`
                    + ` (${dataRecordId},7,${data.winddir}),`
                    + ` (${dataRecordId},8,${data.rainin}),`
                    + ` (${dataRecordId},9,${data.dailyrainin}),`
                    + ` (${dataRecordId},10,${data.solarradiation}),`
                    + ` (${dataRecordId},11,${data.UV}),`
                    + ` (${dataRecordId},12,${data.indoortempf}),`
                    + ` (${dataRecordId},13,${data.indoorhumidity})`;

                //logMessage(sql2); 
                
                const result2 = await link.query(sql2);
                //console.log(result2);

                } catch (error) {
                // Handle DataRecord insertion error
                    throw new Error(`DataRecord insertion failed: ${error.message}`);
                }
        
            }
        }
        if(dry_run){
            logMessage("Dry-run enabled; rolling back DB", 'WARN');
            await link.rollback();
        }else await link.complete();


    }catch(e){
        logMessage('Error: ' + e, 'ERROR');
    }

    link.end();
   
})();


