import TripModel from '../models/trip.js'
import CarModel from '../models/car.js'

// get idle car

export const getIdlecar = (req, res) =>{
    TripModel.getIdlecar(req.params.id, (err, trip)=>{
     if(err)
         res.send(err);
     if(trip.length == 0){
        res.send({status:false, message:'Car Not Available.'})
     }
     else{
        console.log('Trip', trip[0].carID);
        CarModel.getCarByID(trip[0].carID, (err, Car)=>{
            if(err)
                res.send(err);
            console.log('Car', Car);
            if(Car.length == 0){
               res.send({status:false, message:'Car Not Found'})
            }
            else
               res.send({status: true, data:Car})
           })
        }
        
    })
 }