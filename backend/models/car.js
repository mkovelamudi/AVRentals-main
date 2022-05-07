import dbConn from'../config/db.config.js';

var CarModel = function(car){
    this.carID = car.carID,
    this.manufacture = car.manufacture,
    this.model = car.model,
    this.registration_number= car.registration_number;
    this.userID = car.userID;
}

//get all cars
CarModel.getAllCars = (result)=>{
    dbConn.query('SELECT * FROM car',(err, res)=>{
        if(err){
            console.log('Error while fetching cars', err)
            result(err, null)
        }
        else{
            result(null, res);
        }
    })
}

//get carByID from DB
CarModel.getCarByID = (id, result)=>{
    dbConn.query(`SELECT * FROM car WHERE carID=?`, id ,  (err, res)=>{
        if(err){
            console.log('Error while fetching cars', err)
            result(err, null)
        }
        else{
            result(null, res);
        }
    })
}


/* POST JSON Details for Insomnia to create a new car
{
	"manufacture":"Honda",
	"model":"2012",
	"registration_number":MNS123456
    "userID":1
}*/


CarModel.createCar = (carReqData, result) => {
    dbConn.query('INSERT INTO car SET ?', carReqData ,  (err, res)=>{
        if(err){
            console.log('Error while inserting carData', err)
            result(err, null)
        }
        else{
            result(null, res);
        }
    })
}

//get Car Registration Number

CarModel.carRegistration = async (carReqData, result)=>{
    //userReqData.password = await bcrypt.hash(userReqData.password, 10);
    // dbConn.query(`SELECT * FROM users WHERE user_email= and password= ?`,[userReqData.user_email, userReqData.password] ,  (err, res)=>{
        dbConn.query(`SELECT * FROM car WHERE registration_number= ?`,[carReqData.registration_number] ,  async (err, res)=>{
            if(err){
                console.log('Error while fetching user', err)
                result(err, null)
            }
            else{
                result(null, res);
            }
        })
}

//Update car
CarModel.updateCar = (id, carReqData, result) => {
   dbConn.query('UPDATE car SET manufacture=?, model = ?, registration_number = ?, userID = ? WHERE carID = ?',[carReqData.manufacture, carReqData.model, carReqData.registration_number, carReqData.userID, id], (err, res)=>{
   if(err){
            console.log('Error while updating car data', err)
            result(err, null)
        }
        else{
            result(null, res);
        }
    });
}

//Delete car
CarModel.deleteCar = (id, result) => {
    dbConn.query('DELETE FROM car WHERE carID = ?', id, (err, res)=>{
        if(err){
             console.log('Error while deleting car data', err)
             result(err, null)
         }
         else{
             result(null, res);
         }
     });
 }

 //getCarByUser
CarModel.getCarByUser = (userid, result)=>{
    console.log("user ID isn model:",userid)

    //'SELECT sum(total_cost) FROM trip join billing on trip.tripID = billing.tripID  where trip.userID = 2'
    dbConn.query(`SELECT car.carID, car.model as model, car.registration_number as registration_number, car.manufacture as manufacture, count(trip.tripID) as Total_Trips, sum(billing.total_cost) as Revenue, sum(billing.miles) as total_miles FROM car LEFT JOIN trip ON car.carID = trip.carID LEFT JOIN billing ON billing.tripID = trip.tripID where car.userID = ? group by trip.carID`, userid ,  (err, res)=>{        if(err){
            console.log('Error while fetching cars', err)
            result(err, null)
        }
        else{
            // console.log("length is: ",res.data.length)
            result(null, res);
        }
    })
}

//module.exports = car ;
export default CarModel ;
