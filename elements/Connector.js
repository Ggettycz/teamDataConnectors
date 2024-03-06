const fs = require('fs');
const { exec } = require('child_process'); // used for gziping export file
const logMessage = require('../../src/utils.js/logs.js');


class Connector{
	constructor(link, uuid, name, configuration){
        logMessage('-- start of class Connector constructor', 'TRACE');
        this.configurationValidated = false;
        this.link = link;
        this.uuid = uuid;
        this.name = name;
        this.configuration = configuration;
        this.nextRun = 1; // default next Run of connector is one hour
        logMessage('set user id' + configuration.userId, 'TRACE');
        this.userId = configuration.userId || -1; //=== undefined) ? -1 : configuration.userId;
        logMessage('set user controlPlaceId' + configuration.controlPlaceId, 'TRACE');
        this.controlPlaceId = -1;
        this.dataTypeId = -1;
        logMessage('Constructor of new connector with uuid ' + this.uuid, 'TRACE');
    }
    
    async verifyConfiguration(){
        logMessage('starting Connector Configuration verification', 'TRACE');
        this.configurationValidated = await this.verifyMandatoryConfiguration();
        // do any additional Conifiguration here e.g. connector type
        this.configurationValidated &= this.setDefaultConfiguration();
        logMessage('Connector configuration is valid:' + this.configurationValidated, 'INFO');
        logMessage('Confinguration: ' + JSON.stringify(this.configuration), 'DEBUG');
        logMessage('End of Connector Configuration verification', 'TRACE');
        return this.configurationValidated;
    }

    async verifyMandatoryConfiguration(){
        logMessage('starting Connector MANDATORY Configuration verification', 'TRACE');

        let isValidConfig = false;
    
        let query = "SELECT UserID, PlaceID, dtp.DataTypeID, GROUP_CONCAT(dtp.ID SEPARATOR ',') as paramList"
            + " FROM ConnectorPlacesRel cpr INNER JOIN ControlPlaces cp ON cp.ID=cpr.PlaceID"
            + " INNER JOIN DataTypeParameters dtp on cpr.DataTypeId=dtp.DataTypeID"
            + " WHERE cpr.ConnectorID = ?";
        if(this.userId != -1) {
            query += " AND cp.UserID=?";
        }
        query += "  GROUP BY PlaceID";

        logMessage('query:' + query,'TRACE');
    
        const rows = await this.link.query(query,[this.uuid,this.userId]);
        logMessage('Number of Configurations:' + rows.length, 'TRACE');
        if (rows.length==0) {
            logMessage('No valid connector with relations.', 'ERROR');
            return isValidConfig;
        }
        if (rows.length==1) { 
            this.userId = rows[0]['UserID'];
            this.controlPlaceId = rows[0]['PlaceID'];
            this.dataTypeId = rows[0]['DataTypeID'];
            isValidConfig = this.sanitizeParamList(rows[0]['paramList']);
            logMessage(`found user: ${this.userId}, controlPlace: ${this.controlPlaceId}, ` + 
                `dataType: ${this.dataTypeId}, params: ${this.configuration.paramIdsList}`, 'DEBUG');
        } else {
            // we got more than one PlaceId for Connector - invalid configuration 
            // we can change this later
            logMessage('Connector cannot be used for more than one type', 'ERROR');
        }

        if (!isValidConfig) { logMessage('Not valid mandatory configuration for UUID:'+ this.uuid,'ERROR'); }
        logMessage('End of Connector MANDATORY Configuration verification', 'TRACE');

        return isValidConfig;
    }

    setDefaultConfiguration(){
        // there is no default configuration for now for basic connector
        if (this.configuration.activeInterval === undefined) { this.configuration.activeInterval = Connector.DEFAULT_ACTIVE_INTERVAL;}
        logMessage('default configuration set where missing' + this.configurationValidated ,'DEBUG');
        return true;
    }

    setupStartEndDates(){
        // setup the date from/to what we want to start the stats creation
        // required values in configuration: activeInterval
        logMessage('starting Connector setup of Start and End Dates', 'TRACE');

        let toDate = new Date();
        toDate.setHours(toDate.getHours()-this.configuration.activeInterval);
        console.debug('toDate active:',toDate );
        if (this.configuration.toDate !== undefined){
            let configuredToDate = new Date(this.configuration.toDate);
            if (toDate.getTime() > configuredToDate.getTime()) {
                logMessage('switch to configured toDate', 'DEBUG');
                toDate = configuredToDate;
            } else {
                logMessage('configured toDate is overlapping the active interval','ERROR');
                return false;
            }
        }
    
        let fromDate = new Date(0);
        if (this.configuration.fromDate !== undefined){
            fromDate = new Date(this.configuration.fromDate);
            logMessage('switch to configured from date', 'DEBUG');
        }

        if (this.configuration.exportWholeDay) {
            logMessage('whole day has to be exported, changing end/start date', 'DEBUG');
            toDate = new Date(toDate.toISOString());
            fromDate = new Date(fromDate.toISOString());
        }
    
    
        // and verify, fromDate < toDate (if fromDate is sate)
        if (fromDate.getTime() >= toDate.getTime()) {
            logMessage(' from time is bigger than to time','ERROR');
            return  false;
        }
        this.configuration.fromDate = fromDate;
        this.configuration.toDate = toDate;
        logMessage('End of Connector setup of Start and End Dates', 'TRACE');
        return true;
    }

    sanitizeParamList(paramList){
        logMessage('starting Connector param list sanitization', 'TRACE');
        let validParams = paramList.split(',').map(Number);
        if ((this.configuration.paramIdsList !== undefined) &&
            (this.configuration.paramIdsList.length>0)) {
            validParams = validParams.filter(value => this.configuration.paramIdsList.includes(value));
        }
        const isValidConfig = (validParams.length>0);
        this.configuration.paramIdsList = validParams;
        logMessage('Sanitized ParamIdsList: ' + validParams, 'DEBUG');
        logMessage('End of Connector param list sanitization', 'TRACE');
        return isValidConfig;
    }

    async deactivateOldParams(statusId, oldParamIds){
        const result = this.link.query("UPDATE DataRecordNumberParameters SET Active = False, Status=?"
            + " WHERE ID in (?);",[statusId, oldParamIds]);
        return result; // just to have chance to return 
    }

    // should return various statistic e.g. related rows, records etc.
    async getStats(nicePrint = null){
        logMessage('start of Connector getStats','TRACE');
        // get info about (non)active DataRecords related for given controlPlace and DataType
        let query = "SELECT Active, Status, count(id) as Count FROM DataRecords "
            + " WHERE ControlPlaceID=? AND DataTypeID=? group by Active, Status";
        let result =  await this.link.query(query, [this.controlPlaceId,this.dataTypeId]);
        logMessage('controlPlaceActiveStats:' + JSON.stringify(result, null, nicePrint), 'INFO');

        // count data records base don number of remaining parameters assigned to them 
        query = "SELECT count(RecordID) as RecordCount, C as ParamCount FROM "
            + " (SELECT RecordID, count(drnp.id) as c FROM DataRecordNumberParameters drnp"
            + " JOIN DataRecords dr ON RecordID=dr.ID "
            + " WHERE dr.ControlPlaceID=? and dr.DataTypeID=? GROUP BY RecordID) d"
            + " group by c";
        result =  await this.link.query(query, [this.controlPlaceId,this.dataTypeId]);
        logMessage('Parameter count:' + JSON.stringify(result, null, nicePrint), 'INFO');
            
        logMessage('end of Connector getStats','TRACE');

    }

    async finishRun(processed,failed){
        let runStatus;
        if (failed == 0) { runStatus = 'Success' ;}
        else if (processed == 0) { runStatus = 'Failed'; }
        else {runStatus = 'Partial';}
        return this.storeRunStatus(runStatus);
    }
    
    async storeRunStatus(status){
        logMessage(`Storing status (${status}) for connector ${this.uuid}`, 'TRACE');
        const query = "INSERT INTO ConnectorRunStatus (ConnectorID, Status) VALUES (?,?)";
        const statusStr = status || '';
        return this.link.query(query,[this.uuid,statusStr.substring(0,9)], true);
    }

    async updateNextRun( hoursToAdd=1) {
        logMessage(`Updating nextRun by (${hoursToAdd}) for connector ${this.uuid}`, 'TRACE');
        let nextTime = new Date();
        nextTime.setHours(nextTime.getHours()+hoursToAdd);
        const query = "UPDATE Connectors SET NextRun=? WHERE UUID= ?";
        return this.link.query(query,[nextTime,this.uuid]);
    }

    

}

Connector.TO_EXPORT_STATUS = 64; // default status to be exported .. data already aggregated
Connector.DEFAULT_ACTIVE_INTERVAL = 168; // default active interval to NOT be exported and deleted 
Connector.DEFAULT_RECORDS_LIMIT = 10000;
Connector.MAX_SQL_SIZE = 900000; // the max size is more than 1M, just to be sure

module.exports = {
	Connector:Connector
};