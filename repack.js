const {mysql,DBLink} = require('./mysql_creds.js');

const UID = 2;
const DataTypeID = 2;
const ControlPlaceID = 12;
const fromDate = '2024-01-30 00:00:00';
const toDate = '2024-01-30 23:59:59';
const period = 3600 * 1000; //in milliseconds
const oldStatus = 0;
const newStatus = 1;


let dry_run = true;

(async function(){
//=====BEGIN ASYNC WRAPPER=====//
var query  = "select dr.ID,dr.RecordTime,Value as V,dr.MainNumberValue as mv,dn.ParameterTypeID from DataRecords as dr";
	query += " inner join DataRecordNumberParameters as dn on dr.ID = dn.recordID where dr.UserID = ?";
	query += " and dr.DataTypeId = ? and dr.ControlPlaceID = ?";
	query += " and dr.Active = 1 and dr.Status = ?";
	query += " and dr.RecordTime > ? and dr.RecordTime < ?;";


//Init link
const link = new DBLink();
var fromNum = Date.parse(fromDate);

try{
	const groups = [];
	var rows = await link.query(query,[UID,DataTypeID,ControlPlaceID,oldStatus,new Date(fromDate),new Date(toDate)]);	//Do qry
	for(var i=0;i<rows.length;i++){
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
	const OpList = [];
	await link.begin();
	for(var i=0;i<groups.length;i++){
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
	if(dry_run){
		console.warn("Dry-run enabled; rolling back DB");
		await link.rollback();
	}else await link.complete();
}catch(e){
	console.error('Error: ',e);
}
link.end();

//======END ASYNC WRAPPER======//
})();
