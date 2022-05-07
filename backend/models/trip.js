import dbConn from "../config/db.config.js";

var TripModel = function (trip) {
  (this.tripID = trip.tripID),
    (this.dropoff_location = trip.dropoff_location),
    (this.start_time = trip.start_time),
    (this.end_time = trip.end_time),
    (this.pickup_location = trip.pickup_location),
    (this.iscompleted = trip.iscompleted),
    (this.userID = trip.userID),
    (this.carID = trip.carID),
    (this.atPickUp = trip.atPickUp),
    (this.isPickedUp = trip.isPickedUp),
    (this.collision = trip.collision),
    (this.eta = trip.eta);
};

//get all tripss
TripModel.getAllTrips = (result) => {
  dbConn.query("SELECT * FROM trip", (err, res) => {
    if (err) {
      console.log("Error while fetching trips", err);
      result(err, null);
    } else {
      result(null, res);
    }
  });
};

//get tripByID from DB
TripModel.getTripByID = (id, result) => {
  dbConn.query(`SELECT * FROM trip WHERE tripID=?`, id, (err, res) => {
    if (err) {
      console.log("Error while fetching trips", err);
      result(err, null);
    } else {
      result(null, res);
    }
  });
};

TripModel.getIdlecar = (id, result) => {
  // dbConn.query(`SELECT carID FROM trip WHERE iscompleted = 1` ,  (err, res)=>{
  dbConn.query(
    `SELECT carID FROM car WHERE carID NOT IN (SELECT carID FROM trip WHERE iscompleted = 1)`,
    (err, res) => {
      if (err) {
        console.log("Error while fetching trips", err);
        result(err, null);
      } else {
        result(null, res);
      }
    }
  );
};

/* POST JSON Details for Insomnia to create a new trip
{
	"dropoff_location":"Santa CLara",
	"start_time":"2020-01-01 08:10:10",
	"end_time":"2020-01-01 08:30:10",
	"current_location":"San Jose",
	"pickup_location":"San Jose",
	"billingID":1234,
	"userID": 1,
    "iscompleted":1,
    "carID": 1
}*/

TripModel.createTrip = (tripReqData, result) => {
  dbConn.query("INSERT INTO trip SET ?", tripReqData, (err, res) => {
    if (err) {
      console.log("Error while inserting trip data", err);
      result(err, null);
    } else {
      result(null, res);
    }
  });
};

//Update Trip
TripModel.updateTrip = (id, tripReqData, result) => {
  dbConn.query(
    "UPDATE trip SET dropoff_location=?, start_time = ?, end_time = ?, pickup_location = ?, userID = ?, iscompleted = ?, carID = ?, atPickUp = ?, collision = ?, eta = ?, isPickedUp = ? WHERE tripID = ?",
    [
      tripReqData.dropoff_location,
      tripReqData.start_time,
      tripReqData.end_time,
      tripReqData.pickup_location,
      tripReqData.userID,
      tripReqData.iscompleted,
      tripReqData.carID,
      tripReqData.atPickUp,
      tripReqData.collision,
      tripReqData.eta,
      tripReqData.isPickedUp,
      id,
    ],
    (err, res) => {
      if (err) {
        console.log("Error while updating trip data", err);
        result(err, null);
      } else {
        result(null, res);
      }
    }
  );
};

// //Update Pickup
// TripModel.updatePickup = (id, tripReqData, result) => {
//     dbConn.query('UPDATE trip SET dropoff_location=?, start_time = ?, end_time = ?, current_location = ?, pickup_location = ?, userID = ?, carID = ? WHERE tripID = ?',[tripReqData.dropoff_location, tripReqData.start_time, tripReqData.end_time, tripReqData.current_location, tripReqData.pickup_location, tripReqData.userID, tripReqData.carID, id], (err, res)=>{
//     if(err){
//              console.log('Error while updating trip data', err)
//              result(err, null)
//          }
//          else{
//              result(null, res);
//          }
//      });
//  }

//Update Trip Finished
// TripModel.updateFinishedTrip = (tripReqData, result) => {
//     dbConn.query('UPDATE trip SET end_time = NOW(), iscompleted = 1 WHERE tripID = ?',[tripReqData.tripID], (err, res)=>{
//     if(err){
//              console.log('Error while updating trip data', err)
//              result(err, null)
//          }
//          else{
//              result(null, res);
//          }
//      });
//  }

//Update At PickUP
TripModel.updateAtPickUP = (tripReqData, result) => {
  dbConn.query(
    "UPDATE trip SET atPickUP = 1 WHERE tripID = ?",
    [tripReqData.tripID],
    (err, res) => {
      if (err) {
        console.log("Error while updating trip data", err);
        result(err, null);
      } else {
        result(null, res);
      }
    }
  );
};

//Delete Trip
TripModel.deleteTrip = (id, result) => {
  dbConn.query("DELETE FROM trip WHERE tripID = ?", id, (err, res) => {
    if (err) {
      console.log("Error while deleting trip data", err);
      result(err, null);
    } else {
      result(null, res);
    }
  });
};

//get tripByID from DB
TripModel.getTripByUser = (id, result) => {
  dbConn.query(
    `SELECT * FROM 
    billing right join trip 
    inner join car on trip.carID = car.carID
    on trip.tripID = billing.tripID  
    where trip.userID = ? order by trip.tripID desc`,
    id,
    (err, res) => {
      if (err) {
        console.log("Error while fetching trips", err);
        result(err, null);
      } else {
        result(null, res);
      }
    }
  );
};

export default TripModel;
