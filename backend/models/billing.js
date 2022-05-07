import dbConn from'../config/db.config.js';

var BillingModel = function(billing){
    this.billingID = billing.billingID,
    this.miles = billing.miles,
    this.cost= billing.cost,
    this.tax = billing.tax,
    this.total_cost = billing.total_cost,
    this.userID = billing.userID,
    this.tripID = billing.tripID
}

//get all Billings 
BillingModel.getAllBills = (result)=>{
    dbConn.query('SELECT * FROM billing',(err, res)=>{
        if(err){
            console.log('Error while fetching Billing', err)
            result(err, null)
        }
        else{
            console.log('Billing fetched successfully')
            result(null, res);
        }
    })
}

//get BillingByID from DB
BillingModel.getBillingByID = (id, result)=>{
    dbConn.query(`SELECT * FROM billing WHERE billingID=?`, id ,  (err, res)=>{
        if(err){
            console.log('Error while fetching Billing', err)
            result(err, null)
        }
        else{
            console.log('Billing fetched successfully.')
            result(null, res);
        }
    })
}


/* POST JSON Details for Insomnia to create a new Billing
{
	"miles":30,
	"cost":50,
	"tax":4,
	"total_cost":54,
	"userID":1,
	"tripID":1
}*/


BillingModel.createBilling = (BillingReqData, result) => {
    dbConn.query('INSERT INTO billing SET ?', BillingReqData ,  (err, res)=>{
        if(err){
            console.log('Error while inserting Billing Data', err)
            result(err, null)
        }
        else{
            result(null, res);
        }
    })
}

//Update Billing
BillingModel.updateBilling = (id, BillingReqData, result) => {
   dbConn.query('UPDATE billing SET miles=?, cost = ?, tax = ?, total_cost = ?, userID = ?, tripID = ? WHERE billingID = ?',[BillingReqData.miles, BillingReqData.cost, BillingReqData.tax, BillingReqData.total_cost, BillingReqData.tripID, BillingReqData.userID, id], (err, res)=>{
   //dbConn.query('UPDATE Billings SET password=? WHERE BillingID = ?',[BillingReqData.password, id], (err, res)=>{
   if(err){
            console.log('Error while updating BillingData', err)
            result(err, null)
        }
        else{
            result(null, res);
        }
    });
}

//Delete Billing
BillingModel.deleteBilling = (id, result) => {
    dbConn.query('DELETE FROM billing WHERE billingID = ?', id, (err, res)=>{
        if(err){
             console.log('Error while deleting Billing Data', err)
             result(err, null)
         }
         else{
             result(null, res);
         }
     });
 }


//get BillingByID from DB
BillingModel.getBillingByUser = (id, result)=>{
    dbConn.query(`SELECT * FROM billing WHERE userID=?`, id ,  (err, res)=>{
        if(err){
            console.log('Error while fetching Billing', err)
            result(err, null)
        }
        else{
            console.log('Billing fetched successfully.')
            result(null, res);
        }
    })
}
//module.exports = Billing ;
export default BillingModel ;
