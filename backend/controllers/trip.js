import TripModel from "../models/trip.js";
import BillingModel from "../models/billing.js";

import axios from "axios";
import { CARLA_BASE_URL } from "../config/carlaConfig.js";

// get all trips list
export const gettrips = (req, res) => {
  TripModel.getAllTrips((err, trips) => {
    if (err) res.send({ status: false, message: "Trips Not Found" });
    console.log("Trips", trips);
    res.send({ status: true, data: trips });
  });
};

export const getTripByID = (req, res) => {
  TripModel.getTripByID(req.params.id, (err, trip) => {
    if (err) res.send(err);
    console.log("Trip", trip);
    if (trip.length == 0) {
      res.send({ status: false, message: "Trip Not Found" });
    } else res.send({ status: true, data: trip });
  });
};

export const createTrip = (req, res) => {
  let tripid;
  // req.body.start_time = new Date().toISOString().slice(0, 19).replace('T', ' ');
  const tripReqData = new TripModel(req.body);
  //check null
  if (req.body.constructor === Object && Object.keys(req.body).length === 0) {
    res
      .send(400)
      .send({ status: false, message: "Please fill all the fields" });
  } else {
    tripReqData.iscompleted = 0;
    console.log("TripReqData_iscompleted: ", tripReqData.iscompleted);
    console.log("TripReqData after:", tripReqData);
    TripModel.createTrip(tripReqData, (err, trip) => {
      if (err) res.send({ status: false, message: "Trip Not Created." });
      else if (trip != null && trip.affectedRows != 0) {
        console.log(trip);
        (tripid = trip.insertId),
          console.log(
            "trip ID:",
            trip.insertId,
            "car ID:",
            tripReqData.carID,
            "pickup:",
            tripReqData.pickup_location,
            "destination:",
            tripReqData.dropoff_location
          );
        axios
          .post(`${CARLA_BASE_URL}/trip/init`, {
            vehicle_id: tripReqData.carID,
            trip_id: trip.insertId,
            pickup_location: tripReqData.pickup_location,
            destination: tripReqData.dropoff_location,
          })
          .then((response) => {
            console.log("Trip details sent to Carla: ", response);
            res.json({
              status: true,
              message: "Trip Created Successfully",
              data: trip,
            });
          })
          .catch(function (error) {
            console.log("Promise Rejected:", error);
            TripModel.deleteTrip(tripid, (err, trip) => {
              console.log("New Trip added was deleted since carla was failed.");
            });
            res.json({
              status: false,
              message: error.toString(),
            });
          });
      }
    });
  }
};

export const updateTrip = (req, res) => {
  let tripReqData;

  TripModel.getTripByID(req.params.id, (err, trip) => {
    if (err) res.send(err);
    console.log("Trip", trip);
    if (trip.length == 0) {
      res.send({ status: false, message: "Trip Not Found" });
    } else {
      let toUpdate = { ...trip[0], ...req.body };
      if (req.body.iscompleted) {
        toUpdate.end_time = new Date()
          .toISOString()
          .slice(0, 19)
          .replace("T", " ");
        console.log("end time is:", toUpdate.end_time);
        let billObj = {};
        console.log("miles is:", req.body.miles);
        billObj.miles = req.body.miles;
        billObj.cost = billObj.miles * 5;
        billObj.tax = billObj.cost * 0.1;
        billObj.total_cost = billObj.cost + billObj.tax;
        billObj.userID = trip[0].userID;
        billObj.tripID = trip[0].tripID;
        const billingReqData = new BillingModel(billObj);
        BillingModel.createBilling(billingReqData, (err, billing) => {
          if (err) res.send(err);
          console.log(billing);
          console.log("Bill Created.");
          // res.json({status:true, message:'Bill Created Successfully', data:billing})
        });
      }
      tripReqData = new TripModel(toUpdate);
      console.log("tripReqData is:", tripReqData);
      TripModel.updateTrip(req.params.id, tripReqData, (err, trip) => {
        if (err)
          res.send({
            status: false,
            message: "Trip Not Updated. Invalid Values Given.",
          });
        else if (trip.affectedRows == 0)
          res.send({ status: false, message: "Trip Not Found" });
        else res.json({ status: true, message: "Trip Updated Successfully" });
      });
    }
  });
};

export const deleteTrip = (req, res) => {
  TripModel.deleteTrip(req.params.id, (err, trip) => {
    if (err) res.send(err);
    else if (trip.affectedRows == 0)
      res.send({ status: false, message: "Trip Not Found" });
    else res.json({ status: true, message: "Trip Deleted Successfully" });
  });
};

export const updateFinishedTrip = (req, res) => {
  console.log("Trip finished");
  const tripReqData = new TripModel(req.body);
  TripModel.updateFinishedTrip(tripReqData, (err, trip) => {
    if (err)
      res.send({
        status: false,
        message: "Trip Not Updated. Invalid Values Given.",
      });
    else if (trip.affectedRows == 0)
      res.send({ status: false, message: "Trip Not Found" });
    else
      res.json({ status: true, message: "Trip Status Updated to Completed." });
  });
};

export const updateAtPickUP = (req, res) => {
  console.log("Trip finished");
  const tripReqData = new TripModel(req.body);
  TripModel.updateAtPickUP(tripReqData, (err, trip) => {
    if (err)
      res.send({
        status: false,
        message: "AtPickUp Not Updated. Invalid Values Given.",
      });
    else if (trip.affectedRows == 0)
      res.send({ status: false, message: "Trip Not Found" });
    else
      res.json({
        status: true,
        message: "Car Reached at the pickup location.",
      });
  });
};

export const updatePickedup = (req, res) => {
  TripModel.getTripByID(req.body.tripID, (err, trip) => {
    if (err) res.send({ statuse: false, message: err });
    console.log("Trip", trip);
    if (trip.length == 0) {
      res.send({
        status: false,
        message: "Pickup not updated. Trip Not Found",
      });
    } else {
      console.log("Pickup Update:", req.body.tripID);
      axios
        .post(`${CARLA_BASE_URL}/trip/pickup`, {
          trip_id: req.body.tripID,
          crash: req.body.crash,
        })
        .then((response) => {
          console.log("tripID sent to Carla: ", response);
          //res.json({ status: true, message: "Updated Pickedup Successfully" });
          trip[0].start_time = new Date()
            .toISOString()
            .slice(0, 19)
            .replace("T", " ");
          console.log("start Time is:", trip[0].start_time);
          TripModel.updateTrip(trip[0].tripID, trip[0], (err, trip) => {
            if (err)
              res.send({
                status: false,
                message: "Trip Not Updated. Invalid Values Given.",
              });
            else if (trip.affectedRows == 0)
              res.send({ status: false, message: "Trip Not Found" });
            else
              res.json({ status: true, message: "Trip Updated Successfully" });
          });
        })
        .catch(function (error) {
          console.log("Promise Rejected:", error);
          res.json({ status: true, message: error.toString() });
        });
    }
  });
};

export const tripsMadeByUser = (req, res) => {
  TripModel.getTripByUser(req.params.id, (err, trip) => {
    if (err) res.send(err);
    console.log("Trip", trip);
    if (trip.length == 0) {
      res.send({ status: false, message: "Trip Not Found" });
    } else res.send({ status: true, data: trip });
  });
};

export const tripStatus = (req, res) => {
  console.log("trip ID: ", req.params.id);
  axios
    .get(`${CARLA_BASE_URL}/trip/status/${req.params.id}`)
    .then((response) => {
      console.log("Trip status requested to Carla: ", response);
      res.json({ status: true, message: response });
    })
    .catch(function (error) {
      console.log("Promise Rejected:", error);
      res.json({ status: false, message: error.toString() });
    });
};
