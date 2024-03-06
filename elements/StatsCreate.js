const {Connector} = require ('./Connector.js')
const logMessage = require('../../src/utils.js/logs.js');


class StatsCreate extends Connector{
	constructor(link, uuid, configuration){
        super(link, uuid, '-no-', configuration);
        this.type = 'StatsCreate';
        this.nextRun = 24; // next run if success in hours
    }

    async verifyConfiguration(){
        logMessage('starting StatsCreate Configuration verification', 'TRACE');
        this.configurationValidated = await super.verifyMandatoryConfiguration();
        logMessage('mandatory validation result: ' + this.configurationValidated, 'TRACE');
        // do any additional Conifiguration here e.g. connector type
        this.configurationValidated &= this.setDefaultConfiguration();
        logMessage('set default  result: ' + this.configurationValidated, 'TRACE');
        this.configurationValidated &= super.setupStartEndDates();
        logMessage('StatsCreate Connector configuration is valid:' + this.configurationValidated, 'INFO');
        logMessage('Confinguration: ' + JSON.stringify(this.configuration), 'DEBUG');
        logMessage('End of StatsCreate Configuration verification', 'TRACE');
        return this.configurationValidated;
    }

    setDefaultConfiguration(){
        logMessage('starting StatsCreate Default Configuration setup', 'TRACE');
        let isValidConfig = super.setDefaultConfiguration();
        if (this.configuration.oldStatus === undefined) { this.configuration.oldStatus = 0; }
        if (this.configuration.period === undefined) { this.configuration.period = 'hour'; }
        if (this.configuration.recordsLimit === undefined) { this.configuration.recordsLimit = Connector.DEFAULT_RECORDS_LIMIT;}
        logMessage('default StatsCreate configuration set where missing:' + this.configurationValidated ,'DEBUG');
        logMessage('end of StatsCreate Default Configuration setup: ' + isValidConfig, 'TRACE');
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

        let query = this.getAggregationQuery();
        logMessage('sql:' + query, 'TRACE');
        return this.link.query(query,[this.controlPlaceId, this.userId, this.configuration.oldStatus,
            this.configuration.fromDate,this.configuration.toDate, this.configuration.paramIdsList]);

    }

    async runConnector(){
        let recordIds = [];
        let processedIntervals = 0;
        let failedIntervals = 0;
        if (!await this.verifyConfiguration()) {
            logMessage('Configuration for Connector is NOT valid');
            return false;
        }
        logMessage('Provided configuration:' + JSON.stringify(this.configuration), 'DEBUG');
        
        if (!await this.storeRunStatus('Start')) {
            logMessage('Issue with storing the Connector Run Status');
            return false;
        }
        let rows = await this.getRecordsToBeProcessed();
        logMessage('Got ' + rows.length + ' rows, with new statistic information', 'INFO');
    
        for (const row of rows) {
            logMessage('interval' + row['intervalStart'] + ': ' + row['Name'] + '=' + row['AvgValue'], 'DEBUG');
            //... for each row create new DataRecord
            let recordID = await this.createRecord(row['intervalStart'],recordIds);
            if (recordID<0) { // something went wrong try other row
                logMessage('Record ID unavailable ... ', 'ERROR');
                failedIntervals++;
                continue;
            }
            recordID = await this.createRecordParams(recordID,row);
            let oldParamIds = row['IDs'].split(',').map(Number);
            logMessage('Number of oldParamIds' +  oldParamIds.length, 'DEBUG');
            logMessage(oldParamIds +  ',' + typeof(oldParamIds), 'TRACE');
            const deactivate = await this.deactivateOldParams(oldParamIds);
            logMessage('Deactivate successfull: ' + JSON.stringify(deactivate), 'DEBUG');
            processedIntervals++;
        }
        // and add statistic row into correct table
        logMessage(`Run finished, processed intervals: ${processedIntervals}, failed: ${failedIntervals}`, 'INFO');
        let result = await this.finishRun(processedIntervals,failedIntervals);
        let valid = (result.insertId !== undefined); 
        if (!valid) { logMessage('Status of finished run not properly stored', 'ERROR'); }
        result = await this.updateNextRun();
        if ((result.changedRows!=1) || (result.warningCount!=0)) {
            logMessage('Result of Run was not properly stored', 'ERROR');
            valid = false;
        }
        return valid;
    }


    async createRecord(intervalStart,recordIds){
        logMessage('start of StatsCreate create DataRecord', 'TRACE');
        let createdRecordId = recordIds[intervalStart] || -1;
        logMessage('check if we already know recordID: ' + createdRecordId, 'TRACE');
        if (createdRecordId>0) { 
            return createdRecordId;
        }
        // get statsRecord if exists i.e. correct time (intervalStart), ControlPlace and dataTypeId 
        // and Status=STATS_RECORD and Active
       
        const {userId, dataTypeId, controlPlaceId} = this;
        // there shouldnot be more than one .. we will take the latest created just to be sure
        const query = "SELECT ID from DataRecords WHERE RecordTime = ? AND Status= ? AND Active=1" // using intervalStart and STATS_RECORD
            + " AND UserID = ? AND ControlPlaceID = ? AND DataTypeID = ?" //
            + " ORDER BY CreatedTime DESC LIMIT 1";
        logMessage('query prepared:' + query, 'TRACE');
        const rows = await this.link.query(query,
                [intervalStart,StatsCreate.STATS_RECORD_STATUS,userId, controlPlaceId, dataTypeId]);
        logMessage('rows returned: ' + rows.length + ',' + JSON.stringify(rows), 'TRACE');
        if (rows.length>0){ // there should be only one row as we limited it to 1
            createdRecordId = rows[0]['ID'];
            logMessage('existing DataRecord found with ID:' + createdRecordId, 'DEBUG');
        } else {
            // record doesn't exist ... we have to create new one
            const query2 =  "INSERT INTO DataRecords (DataTypeId,UserID,ControlPlaceID,Status,RecordTime) "
                + " VALUES(?,?,?,?,?);"
            const result = await this.link.query(query2,[dataTypeId, userId, controlPlaceId, StatsCreate.STATS_RECORD_STATUS, intervalStart]);
            if (result.insertId !== undefined) {
                logMessage('New record created:' + result.insertId, 'DEBUG');
                createdRecordId = result.insertId;
                recordIds[intervalStart] = createdRecordId;
            }
        }
        logMessage('end of StatsCreate create DataRecord', 'TRACE');
        return createdRecordId;
    }

    async createRecordParams(recordID, row){
        const { ParamTypeId, AvgValue,MinValue,MaxVal,StdDevVal} = row;
        const query = "INSERT INTO DataRecordParameterStats (ParameterTypeID,RecordID,AvgVal,MinVal,MaxVal,StdDevVal,Status)"
            + " VALUES (?,?,?,?,?,?,?)";
        const result = this.link.query(query,[ParamTypeId, recordID, AvgValue,MinValue,MaxVal,StdDevVal,StatsCreate.STATS_RECORD_STATUS]);
        logMessage(`New RecordParam (${result.insertId}) created for Record: ${recordID} and ParamTypeId:${ParamTypeId}`, 'TRACE');
        return result.insertId;
    }

    
    
    async deactivateOldParams(oldParamIds){
        return super.deactivateOldParams(StatsCreate.STATS_RECORD_STATUS, oldParamIds);
    }
    

    /** 
     * prepare query based on required granularity .. i.e. period for which the values are agregated
     * @returns {string} query with parametrization:
     *         [controlPlaceId, userId,oldStatus,fromDateParam,new Date(toDate), paramIdsList]);
     *          values returned by query: ParamTypeId, RecordTime, intervalStart, Name,AvgValue,MinValue,MaxVal,StdDevVal, IDs
     */
    getAggregationQuery() {
        // for now the period can be either hour or minute ... if the period is not minute .. it will be set for hour
        // period has to be set in the configuration as it is prepared by setDefaultConfiguration
        const period = this.configuration.period; 
        const formatString = (period == 'minute') ? '%Y-%m-%d %H:%i:00' : '%Y-%m-%d %H:00:00';
        let query = "SELECT ParamTypeId, RecordTime, intervalStart, Name,AvgValue,MinValue,MaxVal,StdDevVal, IDs FROM DataTypeParameters dtp JOIN"
            + " (SELECT dp.ParameterTypeID as ParamTypeId, intervalStart, RecordTime, ROUND(AVG(Value),2) as AvgValue, ROUND(MIN(Value),2) as MinValue,"
            + " ROUND(MAX(Value),2) as MaxVal, STDDEV(Value) as StdDevVal,"
            + " GROUP_CONCAT(dp.ID ORDER BY dp.ID SEPARATOR ',') as IDs FROM DataRecordNumberParameters dp JOIN"
            + " (SELECT DISTINCT dr.ID, RecordTime,"
                + " DATE_FORMAT(dr.RecordTime, '" + formatString + "') AS intervalStart"
                + " FROM DataRecords dr WHERE dr.ControlPlaceID = ? AND UserID=?" // using controlPlaceID and userID
                + " AND dr.Active=1 AND Status = ?" // using oldStatus
                + " AND dr.RecordTime BETWEEN ? AND ? " // using formDate, toDate
            + ") da ON da.ID = dp.RecordID"
            + " WHERE dp.Active=1 AND dp.ParameterTypeID IN (?) GROUP BY intervalStart,  dp.ParameterTypeID " // using paramIdsList
            + ") dx on dx.ParamTypeId=dtp.ID";
        return query;
    }

}

StatsCreate.STATS_RECORD_STATUS = 64;

module.exports = {
	StatsCreate:StatsCreate
};