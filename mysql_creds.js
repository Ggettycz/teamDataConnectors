const mq = require('mysql');
class DBLink{
	constructor(){
		this.link = mq.createConnection({
				host: 'localhost',
				user: 'teamDataDB',
				password: 'dataTeam_p4ss',
				database: 'teamData'
			});
		this.tstate = DBLink.STATE_TRANS_IDLE;
		this.TO = 60000;
	}
	query(sql,vals){
		return new Promise((complete,fail)=>{
			const result = this.link.query({
				sql:sql,
				timeout:this.TO,
				values:vals
				},(e,r,f)=>{
					if(e)
						fail(e);
					else 
						complete(r,f);
				});
			console.log(result.sql);
		});
	}
	end(){
		if(this.tstate == DBLink.STATE_TRANS_ACTIVE)
			this.rollback();
		this.link.end();
	}

	begin(){
		if(this.tstate != DBLink.STATE_TRANS_IDLE)
			throw "Transaction state error";
		this.tstate = DBLink.STATE_TRANS_START;
		const _T = this;
		return new Promise((complete,fail)=>{
			this.link.beginTransaction(function(E){
				if(E){
					_T.tstate = DBLink.STATE_TRANS_IDLE;
					fail(E);
				}else{
					_T.tstate = DBLink.STATE_TRANS_ACTIVE;
					complete();
				}
			});
		});
	}
	complete(){
		if(this.tstate != DBLink.STATE_TRANS_ACTIVE)
			throw "Transaction state error";
		this.tstate = DBLink.STATE_TRANS_ENDING;
		return new Promise((complete,fail)=>{
			this.link.commit(function(E){
				if(E){
					this.tstate = DBLink.STATE_TRANS_ACTIVE;
					fail(this.rollback());
				}else{
					this.tstate = DBLink.STATE_TRANS_IDLE;
					complete();
				}
			});
		});
	}
	rollback(){
		if(this.tstate != DBLink.STATE_TRANS_ACTIVE)
			throw "Transaction state error";
		this.tstate = DBLink.STATE_TRANS_ENDING;
		return new Promise((complete,fail)=>{
			this.link.rollback(function(E){
				this.tstate = DBLink.STATE_TRANS_IDLE;
				if(E){
					fail(E);
				}else{
					complete();
				}
			});
		})
	}
}
DBLink.STATE_TRANS_IDLE = 0,
DBLink.STATE_TRANS_START = 1,
DBLink.STATE_TRANS_ACTIVE = 2,
DBLink.STATE_TRANS_ENDING = 3
module.exports = {
	mysql: mq,
	DBLink:DBLink
};
