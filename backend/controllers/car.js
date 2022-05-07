import CarModel from "../models/car.js";
import axios from "axios";
import { CARLA_BASE_URL } from "../config/carlaConfig.js";

// get all Cars list
export const getcars = (req, res) => {
  CarModel.getAllCars((err, Cars) => {
    if (err) res.send({ status: false, message: err });
    console.log("Cars", Cars);
    res.send({ status: true, data: Cars });
  });
};

export const getCarByID = (req, res) => {
  CarModel.getCarByID(req.params.id, (err, Car) => {
    if (err) res.send(err);
    console.log("Car", Car);
    if (Car.length == 0) {
      res.send({ status: false, message: "Car Not Found" });
    } else res.send({ status: true, data: Car });
  });
};

export const createCar = (req, res) => {
  const CarReqData = new CarModel(req.body);
  let carid 
  //check null
  if (req.body.constructor === Object && Object.keys(req.body).length === 0) {
    res
      .send(400)
      .send({ status: false, message: "Please fill all the fields" });
  } else {
    CarModel.carRegistration(CarReqData, (err, carRegistration) => {
      if (err) res.send({ status: false, message: err });
      if (carRegistration.length == 0) {
        CarModel.createCar(CarReqData, (err, Car) => {
          if (err) res.send({ status: false, message: err });
          //axios.post('http://1e72-73-15-187-30.ngrok.io/vehicle', {"vehicle_id":CarReqData.carID}).then((response) => {
          else if (Car != null && Car.affectedRows != 0) {
            console.log("Car:", Car);
            carid = Car.insertId;
            console.log("carIDSent: ", Car.insertId);
            axios
              .post(`${CARLA_BASE_URL}/vehicle`, {
                vehicle_id: Car.insertId,
              })
              .then((response) => {
                console.log("CarID sent to Carla: ", response);
                res.json({
                  status: true,
                  message: "Car Added Successfully",
                  data: Car,
                });
              })
              .catch(function (error) {
                console.log("Promise Rejected:", error);
                CarModel.deleteCar(carid, (err, Car) => {
                  console.log("Newly Added car deleted because of carla failure.")
                })
                res.json({
                  status: false,
                  message: error.toString()
                });
              });
          }
        });
      } else {
        res.send({
          status: false,
          message:
            "Car cannot be Added. Car with the same registration number already exists.",
        });
      }
    });
  }
};

export const updateCar = (req, res) => {
  const CarReqData = new CarModel(req.body);
  //check null
  if (req.body.constructor === Object && Object.keys(req.body).length === 0) {
    res
      .send(400)
      .send({ status: false, message: "Please fill all the fields" });
  } else {
    CarModel.updateCar(req.params.id, CarReqData, (err, Car) => {
      if (err)
        res.send({
          status: false,
          message: "Car Not Updated. Invalid Values Given.",
        });
      else if (Car.affectedRows == 0)
        res.send({ status: false, message: "Car Not Found" });
      else res.json({ status: true, message: "Car Updated Successfully" });
    });
  }
};

export const deleteCar = (req, res) => {
  axios
        .delete(`${CARLA_BASE_URL}/vehicle/${req.params.id}`)
        .then((response) => {
          CarModel.deleteCar(req.params.id, (err, Car) => {
            if (err) res.send(err);
            else if (Car.affectedRows == 0)
              res.send({ status: false, message: "Car Not Found" });
            else res.json({ status: true, message: "Car Deleted Successfully" });
          });
        })
        .catch(function (error) {
          console.log("Promise Rejected:", error);
          res.json({
            status: false,
            message: error.toString()
          });
        });
};

export const getIdleCar = (req, res) => {
  let carID = "";
  console.log("current Location is:", req.body.current_location);
  axios
    .get(`${CARLA_BASE_URL}/trip/nearby?location=${req.body.current_location}`)
    .then((response) => {
      console.log("Carla Response", response["data"][0]["vehicle_id"]);
      CarModel.getCarByID(response["data"][0]["vehicle_id"], (err, Car) => {
        if (err) res.send(err);
        console.log("Car", Car);
        if (Car.length == 0) {
          res.send({ status: false, message: "Car Not Found" });
        } else res.send({ status: true, data: Car });
      });
    })
    .catch(function (error) {
      console.log("Promise Rejected:", error);
      res.send({ status: false, message: "Car Not Found" });
    });
};

export const getCarByUser = (req, res) => {
  CarModel.getCarByUser(req.params.id, (err, Car) => {
    if (err) res.send(err);
    console.log("Car BY User", Car);
    if (Car.length == 0) {
      res.send({ status: false, message: "Car Not Found" });
    } else res.send({ status: true, data: Car });
  });
};
