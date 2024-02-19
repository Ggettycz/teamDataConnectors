const {mysql,DBLink} = require('./mysql_creds.js');

const connectorTypes = ''; //"StatsCreate"';

(async function(){
    // simple select so no transaction needed.
    const link = new DBLink();
    let query  = "SELECT UUID, Name, ConnectorType, Configuration FROM Connectors"
        + " WHERE Active=1 AND NextRun < NOW()";
    if (connectorTypes != '') {
        sql += " AND ConnectorType in ( ? )";
    }

    let rows = await link.query(query,[connectorTypes]);	//Do qry

    for (const row of rows) {
        let uuid = row['UUID'];
        let connectorType = row['ConnectorType'];
        let configuration = row['Configuration'];
        // run connector based on the type
        // update Next Run if successful
        if (connectorType == 'StatsCreate') {
            await statsCreate(link,uuid,configuration);
        }
    }
    //======END ASYNC WRAPPER======//
    link.end();
   
})();

async function statsCreate(link, uuid, configuration) {
    // get parameters from configuration and DB to be able proces single statsCreate connector
    // get values to be aggregated
    // jeste mi tady neco chybi .. akorat ted nevim co
    configuration = {
        'userId':2, 
        'paramIdsList':[2,8,12,54],
        'period':'hour',
        'toDate':'2024-02-01'
        //'fromDate': '2024-01-03'
    };

    const {valid,userId,controlPlaceId,paramIdsList} = await statsCreateConfig(link,uuid, configuration);
    if (!valid) {
        console.error('Not valid configuration for UUID:'+ uuid);
        return;
    }
    console.log('- ', valid, ' userId: ', userId);
    console.log('- controlPlaceID:    ', controlPlaceId);
    console.log('- paramIdsList:  ', paramIdsList);

    const {validDef, oldStatus, fromDate, toDate, period, intervalFormat} = statsCreateDefaultConfig(configuration);
    console.log('- ', validDef, ' oldStatus: ', oldStatus);
    console.log('- period:    ', period);
    console.log('- fromDate:  ', fromDate, ', ', fromDate.getTime());
    console.log('- toDate:    ', toDate);
    console.log('- intervalFormat:    ', intervalFormat);
    if (!validDef) {
        console.error('Not valid default configuration for UUID:'+ uuid);
        return;
    }
    
    let dry_run = true;

    console.log('---------start ing');

    const query = statsCreatePrepareQuery(intervalFormat);
        
    try{
        const groups = [];
        const fromDateParam = new Date(fromDate)
        let rows = await link.query(query,[controlPlaceId, userId,oldStatus,fromDateParam,new Date(toDate), paramIdsList]);	//Do qry
        for (const row of rows) {
            console.log(row['intervalStart'],row['Name'],': ',row['AvgValue']);
        }

        /*for(var i=0;i<rows.length;i++){
            var row = rows[i];
            var T = Math.floor((row.RecordTime - fromNum)/period);					//Lookup row
            if(!groups[T]){
                groups[T] = {ids:[],params:[],mv_sum:0}
            }
            var o = groups[T];
            if(o.ids.indexOf(row.ID)==-1){
                o.ids.push(row.ID);													//Create list of IDs per entry
                o.mv_sum += row.mv;													//   and sum up MainNumberValue
            }
            var E = o.params[row.ParameterTypeID];
            if(!E) E = o.params[row.ParameterTypeID] = {count:0,sum:0};				//Sum up all IDs
            E.count++;
            E.sum += row.V;
        }
        */
        await link.begin();
        /*for(var i=0;i<groups.length;i++){
    //		console.log('Date: ' + new Date(i * period + fromNum).toISOString());
    //		console.log('ID count: '+groups[i].ids.length);
            var p = groups[i].params;
            //Create datarecord
            let qry2 = "INSERT INTO DataRecords (DataTypeId,ParentId,MainNumberValue,UserID,ControlPlaceID"
                qry2+= ",Status,RecordTime) VALUES(?,0,?,?,?, ?,?);"
            await link.query(qry2,[DataTypeID, groups[i].mv_sum / groups[i].ids.length,UID,ControlPlaceID,newStatus,new Date(i * period + fromNum)])
            const newRecord = (await link.query("SELECT LAST_INSERT_ID()as newID;"))[0].newID
            for(var j=0;j<p.length;j++){
                if(p[j]){
                    await link.query("INSERT INTO DataRecordNumberParameters (ParameterTypeID,RecordID,Value,Active,Status) VALUES (?,?,?,1,?)",
                        [j,newRecord,p[j].sum / p[j].count,newStatus]);
                }
            }
            link.query("UPDATE DataRecords SET Active = False where id in (?);",[groups[i].ids]);
        }
        */
        if(dry_run){
            console.warn("Dry-run enabled; rolling back DB");
            await link.rollback();
        }else await link.complete();
    }catch(e){
        console.error('Error: ',e);
    }
    console.log('---------end of');
}

async function statsCreateConfig(link,uuid, configuration) {
    console.debug('inside statsCreateConfig');
    let userId = configuration.userId;
    if (userId == undefined) { userId = -1; }
    console.debug('(not) found user:' + userId);
    let paramIdsList = configuration.paramIdsList;
    if (paramIdsList == undefined) {paramIdsList = [];}
    console.debug('(not) found paramIdsList:' + paramIdsList);
    let validParams = [];
    // we have to get userId (if not configured), controlPlaceId from DB and filter the paramIdsList provided in configuration
    // a5e53662-ce8a-11ee-9d77-9009d01d7a9f
    // !! be aware, that connector are now not connected to the user dirctly, but over the controlPlace.
    // only one user and one ControlPlace can be returned - if there are more of them, it is not valid configuration
    isValidConfig = false;

    query = "SELECT UserID, PlaceID, dtp.DataTypeID, GROUP_CONCAT(dtp.ID SEPARATOR ',') as paramList"
        + " FROM ConnectorPlacesRel cpr INNER JOIN ControlPlaces cp ON cp.ID=cpr.PlaceID"
        + " INNER JOIN DataTypeParameters dtp on cpr.DataTypeId=dtp.DataTypeID"
        + " WHERE cpr.ConnectorID = ?";
    if(userId != -1) {
        query += " AND cp.UserID=?";
    }
    query += "  GROUP BY PlaceID";

    const rows = await link.query(query,[uuid,userId]);
    console.log('rows:' + rows.length);
    let validParams2;
    if (rows.length==1) { 
        console.log('valid configuration');
        userId = rows[0]['UserID'];
        controlPlaceId = rows[0]['PlaceID'];
        validParams = rows[0]['paramList'].split(',').map(Number).filter(value => paramIdsList.includes(value));
        isValidConfig = (validParams.length>0)? true: false;

    } else {
        // we got more than one PlaceId for Connector - invalid configuration 
        // we can change this later
        console.error('Connector cannot be used for more than one type');
    }
    return { 'valid': isValidConfig, 'userId': userId,'controlPlaceId': controlPlaceId,'paramIdsList': validParams};
}

function statsCreateDefaultConfig(configuration){
    let isValidConfig = true;
    const oldStatus = (configuration.oldStatus === undefined) ? 0 : configuration.oldStatus;
    // default period is one hour .. i.e. 3600 sec
    const period = (configuration.period === undefined) ? 'hour' : configuration.period;
    // default active interval is 7 days i.e. 24*7
    const activeInterval = (configuration.activeInterval === undefined) ? 168 : configuration.activeInterval;
    // setup the date from/to what we want to start the stats creation
    let toDate = new Date();
    toDate.setHours(toDate.getHours()-activeInterval);
    if (configuration.toDate !== undefined){
        configuredToDate = new Date(configuration.toDate);
        if (toDate.getTime() > configuredToDate.getTime()) {
            console.log('switch to configured toDate');
            toDate = configuredToDate;
        } else {
            console.error('configured toDate is overlapping the active interval');
            isValidConfig = false;
        }
    }
    toDate = new Date(toDate.toISOString());

    let fromDate = new Date(0);
    if (configuration.fromDate !== undefined){
        fromDate = new Date(configuration.fromDate);
        console.log('switch to configured fromDate');
    }

    fromDate = new Date(fromDate.toISOString());

    // and verify, fromDate < toDate (if fromDate is sate)
    if (fromDate.getTime() > toDate.getTime()) {
        console.error(' from time is bigger than to time');
        isValidConfig = false;
    }
    console.log(' ---- is valid ', isValidConfig);
    return { 'validDef':isValidConfig, oldStatus, fromDate, toDate, period};
}

/** 
 * prepare query based on required granularity .. i.e. period for which the values are agregated
 * @param {string} period (string 'minute' or 'hour')
 * @returns {string} query with parametrization:
 *         [controlPlaceId, userId,oldStatus,fromDateParam,new Date(toDate), paramIdsList]);	//Do qry
 */
function statsCreatePrepareQuery(period) {
    // for now the period can be either hour or minute ... if the period is not minute .. it will be set for hour
    const formatString = (period == 'minute') ? '%Y-%m-%d %H:%i' : '%Y-%m-%d %H';
    let query = "SELECT PTID, RecordTime, intervalStart, Name,AvgValue,MinValue,MaxVal,StdDevVal, IDs FROM DataTypeParameters dtp JOIN"
        + " (SELECT dp.ParameterTypeID as PTID, intervalStart, RecordTime, ROUND(AVG(Value),2) as AvgValue, ROUND(MIN(Value),2) as MinValue,"
         + " ROUND(MAX(Value),2) as MaxVal, STDDEV(Value) as StdDevVal,"
         + " GROUP_CONCAT(dp.ID ORDER BY dp.ID SEPARATOR ',') as IDs FROM DataRecordNumberParameters dp JOIN"
           + " (SELECT DISTINCT dr.ID, RecordTime,"
             //+ " FLOOR((TIME_TO_SEC(dr.RecordTime) - TIME_TO_SEC(?))/?) as intervalStart";
             + " DATE_FORMAT(dr.RecordTime, '" + formatString + "') AS intervalStart"
             + " FROM DataRecords dr WHERE dr.ControlPlaceID = ? AND UserID=?" // using controlPlaceID and userID
             + " AND dr.Active=1 AND Status = ?" // using oldStatus
             + " AND dr.RecordTime BETWEEN ? AND ? " // using formDate, toDate
           + ") da ON da.ID = dp.RecordID"
         + " WHERE dp.ParameterTypeID IN (?) GROUP BY intervalStart,  dp.ParameterTypeID " // using paramIdsList
        + ") dx on dx.PTID=dtp.ID";
    return query;
}