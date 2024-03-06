const fs = require('fs');
const { exec } = require('child_process'); // used for gziping export file
const logMessage = require('../../src/utils.js/logs.js');
const {Connector} = require ('./Connector.js')


class FileExport extends Connector{
	constructor(link, uuid, name, configuration){
        logMessage('-- start of class FileExport constructor', 'TRACE');
        super(link, uuid,name,configuration);
        this.processedIDs = [];
    }
    
    async verifyConfiguration(){
        this.configurationValidated = await super.verifyMandatoryConfiguration();
        // do any additional Conifiguration here e.g. connector type
        this.configurationValidated &= this.setDefaultConfiguration();
        this.configurationValidated &= super.setupStartEndDates();
        logMessage('FileExport Connector configuration is valid:' + this.configurationValidated, 'INFO');
        logMessage('Confinguration: ' + JSON.stringify(this.configuration), 'DEBUG');
        return this.configurationValidated;
    }

    setDefaultConfiguration(){
        // there is no default configuration for now for basic connector class
        let isValidConfig = super.setDefaultConfiguration();
        logMessage('actual configuration 1 : ' + JSON.stringify(this.configuration), 'TRACE');
        if (this.configuration.parameterStatus === undefined) { this.configuration.parameterStatus = FileExport.TO_EXPORT_STATUS; }
        if (this.configuration.exportWholeDay === undefined) { this.configuration.exportWholeDay = FileExport.DEFAULT_WHOLE_DAY_EXPORT;}
        if (this.configuration.exportActive === undefined) { this.configuration.exportActive = FileExport.DEFAULT_EXPORT_ACTIVE;}
        if (this.configuration.recordsLimit === undefined) { this.configuration.recordsLimit = Connector.DEFAULT_RECORDS_LIMIT;}
        if (this.configuration.dataPath === undefined) { this.configuration.dataPath = FileExport.dataPath;}
        logMessage('default configuration set where missing:' + this.configurationValidated ,'DEBUG');
        logMessage('actual configuration: ' + JSON.stringify(this.configuration), 'TRACE');
        return isValidConfig;
    }

    sanitizeParamList(paramList){
        logMessage('starting FileExport sanitizeParamList','TRACE');
        let validParams = paramList.split(',').map(Number);
        if ((this.configuration.paramIdsList !== undefined) &&
            (this.configuration.paramIdsList !== null) &&
            (this.configuration.paramIdsList.length>0)) {
            validParams = validParams.filter(value => this.configuration.paramIdsList.includes(value));
        }
        const isValidConfig = (validParams.length>0);
        this.configuration.paramIdsList = validParams;
        logMessage('Sanitized ParamIdsList: ' + validParams, 'DEBUG');
        return isValidConfig;
    }


    async getRecordsToBeProcessed(){ 
        logMessage('--starting get Records', 'TRACE');
        // for now we need two things ... record IDs to delete and data to store
        // to know what all has to be processed .. splitted by period, just IDs & counts
        if ((!this.configurationValidated) && (! await this.verifyConfiguration())) {
            logMessage('Invalid configuration','ERROR');
            return null;
        }
        let query = "SELECT drnp.ID as paramID, drnp.ParameterTypeID as ParamTypeID, drnp.Value, "
            + " DATE_FORMAT(dr.RecordTime, '%H:%i:%s') AS timeStr,"
            + " DATE_FORMAT(dr.RecordTime, '%Y-%m-%d') AS dateStr"
            + " FROM DataRecordNumberParameters drnp"
            + " JOIN DataRecords dr ON dr.ID = drnp.RecordID "
            + " WHERE ParameterTypeID in (?)" // using paramIdsList
            + " AND (dr.RecordTime BETWEEN ? AND ? )" // using formDate, toDate

        if (this.configuration.parameterStatus !== null) {
            query += " AND drnp.Status=" + this.configuration.parameterStatus;
        }
    
        if (this.configuration.exportActive !== null) {
            query += " AND drnp.Active=" + this.configuration.exportActive;
        }
        query += " ORDER BY dateStr, ParameterTypeID, timeStr ASC ";
        if (this.configuration.recordsLimit) {
            query += " LIMIT ?"; // using recordsLimit
        }
        logMessage('sql:' + query, 'TRACE');
        return this.link.query(query,[this.configuration.paramIdsList,
                this.configuration.fromDate, this.configuration.toDate, this.configuration.recordsLimit], true);

    }


    prepareBasicStructure(rows){
        logMessage('.. start of prepareBasicStructure', 'TRACE');
        const exportData = { export: [] };

        rows.forEach(row => {
            logMessage('get exportObj:', 'TRACE');
            let exportObj = exportData.export.find(obj => obj.date === row.dateStr);
            if (!exportObj) {
                exportObj = { date: row.dateStr, data: [] };
                exportData.export.push(exportObj);
            }
            logMessage('prepare dataObj:', 'TRACE');

            let dataObj = exportObj.data.find(obj => obj.paramID === row.ParamTypeID);
            if (!dataObj) {
                logMessage('create new dataObj', 'TRACE');
                dataObj = { paramID: row.ParamTypeID, values: {} };
                exportObj.data.push(dataObj);
            }

            logMessage(`set value: ${row.timeStr}, ${row.Value},` + JSON.stringify(dataObj.values) , 'TRACE');
            dataObj.values[row.timeStr] = row.Value;
            this.processedIDs.push(row.paramID);
           
        });
        let response = {};
        response.export = exportData;
        return response;

    }

    prepareTimeSeriesStructure(rows){
        let timeObj = [];
        let valueObj = [];
        let IDs = [];
        for (const row of rows){
            const {paramID, ParamTypeID, Value,timeStr,dateStr} = row;

            timeObj[dateStr] = timeObj[dateStr] || [];
            timeObj[dateStr][ParamTypeID] = timeObj[dateStr][ParamTypeID] || [];
            timeObj[dateStr][ParamTypeID].push(timeStr);

            valueObj[dateStr] = valueObj[dateStr] || [];
            valueObj[dateStr][ParamTypeID] = valueObj[dateStr][ParamTypeID] || [];
            valueObj[dateStr][ParamTypeID].push(Value);

            IDs.push(paramID)
    
        }

        let response = {};
        response.IDs = IDs;
        response.export = [];
        for (const dateKey in timeObj){
            let dateObj = {};
            dateObj.date = dateKey;
            dateObj.data = [];
            for (const paramKey in timeObj[dateKey]){
                dateObj.data.push({
                    'paramID' : paramKey,
                    'times':timeObj[dateKey][paramKey],
                    'values':valueObj[dateKey][paramKey],
                    });
            }
            response.export.push(dateObj);
        }

        return response;

    }

    async exportDataToFile(records,controlPlaceName = '', paramNames = []){
        // export format for now .. each file for controlPlace,period,
        // all processed params 
        if (this.noIDsToProcess()) {return false;}
        let fileStructure = {
            'connectorUUID': this.uuid,
            'controlPlaceID': this.controlPlaceId,
            'dataTypeId': this.dataTypeId, // not sure if needed
            'deletedFromDB' : this.configuration.deleteAfterExport
        };
        // we don't need and use it now .. but it can be necessary or required
        if ((controlPlaceName !== undefined) && (controlPlaceName != '')){
            fileStructure.controlPlaceName = controlPlaceName;
        }
        if ((paramNames !== undefined) && paramNames.length>0){
            fileStructure.paramNames = [];
            for (const ptID in paramNames){
                fileStructure.paramNames.push({'paramTypeID': ptID, 'paramName': paramNames[ptID]});
            }
        }

        fileStructure.export = records.export;
        return  this._writeToFile(JSON.stringify(fileStructure, null, 2)); 


    }

    _writeToFile(jsonString){
        // prepare file name based on connector name and date time, 
        // add the path if necessary it can be for each user differnet in future ... 
        let fileName = this.configuration.fileName || this.name.replace(/[^a-zA-Z0-9_-]/g, '');
        fileName += '_' + new Date().toISOString().replace(/[-T:.Z]/g, '') + '.json'; 
        this.configuration.fileName = fileName;
        
        fs.writeFile(FileExport.dataPath + fileName, jsonString, err => {
            if (err) {
                logMessage('Error writing file:' + err.message,'ERROR');
                return false;
            }
            logMessage('JSON data has been saved to ' + fileName, 'INFO');
        });
        return true;
    }

    async compressExportedFile(){
        return new Promise((resolve, reject) => {
            const fileName = FileExport.dataPath + this.configuration.fileName;
            exec(`gzip ${fileName}`, (error, stdout, stderr) => {
                if (error) {
                    logMessage(`Compression failed: ${error.message}`, 'ERROR');
                    reject(error);
                }
                if (stderr) {
                    logMessage(`Compression error: ${stderr}`,'ERROR');
                    reject(new Error(stderr));
                }
                logMessage(`File compressed successfully: ${fileName}`, 'INFO');
                resolve();
            });
        });
    }


    noIDsToProcess(){
        return this.processedIDs.length<1;
    }

    async deleteProcessedData(){
        if (this.noIDsToProcess()) {return false}
        const query = "DELETE from DataRecordNumberParameters where ID in (?)"
        const result = await this.link.query(query,[this.processedIDs]);
        return result.affectedRows;
    }

    async helper(toHelp){
        let query='';
        let result;
        switch(toHelp) {
            case 'getParamIDsCount':
                query = "SELECT count(ID) as count from DataRecordNumberParameters";
                break;
            case 'dbStats':
                query = "SELECT count(ID) as count, ParameterTypeID, Active,Status from DataRecordNumberParameters";
                query += " GROUP BY ParameterTypeID, Active, Status";
                break;
            case 'dbStatsNotActiveStatsCreated':
                query = "SELECT count(ID) as count, ParameterTypeID from DataRecordNumberParameters"
                    + " WHERE Active=0 AND Status="+Connector.TO_EXPORT_STATUS
                    + " GROUP BY ParameterTypeID";
                break;


        }
        result = await this.link.query(query);
        console.log(result);
        return result;
    }

}

FileExport.DEFAULT_WHOLE_DAY_EXPORT = true; 
FileExport.DEFAULT_EXPORT_ACTIVE = false;

FileExport.EXPORT_STRUCTURE_BASIC = 'Basic';
FileExport.EXPORT_STRUCTURE_TIMESERIES = 'TimeSeries';
FileExport.dataPath = '../data/export/';



module.exports = {
	FileExport:FileExport
};