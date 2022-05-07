import { createSlice } from "@reduxjs/toolkit";

const initialState = {
  users: [],
  trips: [],
  billings: [],
  cars: []
}

export const adminSlice = createSlice({
  name: 'admin',
  initialState, 
  reducers: {
    updateUsers: (state, { payload }) => ({
      ...state,
      ...{ users: payload.users },
    }),
    updateUser: (state, {payload}) => {
      const { user: newUser } = payload;
      const userIndex = state.users.findIndex(({userID}) => userID === newUser.userID);
      if (userIndex !== -1) {
        const users = [...state.users];
        users[userIndex] = newUser;
        return {
          ...state,
          users
        }
      }
    },
    deleteUser: (state, { payload}) => {
      const { userID } = payload;
      const userIndex = state.users.findIndex((user) => user.userID === userID);
      if (userIndex !== 1) {
        const newUsers = [...state.users];
        newUsers.splice(userIndex, 1)
        return ({
          ...state,
          users: newUsers
        })
      }
    },
    updateTrips: (state, {payload}) => ({
      ...state,
      trips: payload.trips
    }),
    updateBillings: (state, {payload}) => ({
      ...state,
      billings: payload.billings
    }),
    updateCars: (state, {payload}) => ({
      ...state,
      cars: payload.cars
    }),
    updateCar: (state, {payload}) => {
      const { car: newCar } = payload;
      const carIndex = state.cars.findIndex(({carID}) => carID === newCar.carID);
      console.log("Updating car state with new car: ", newCar)
      if (carIndex !== -1) {
        const cars = [...state.cars];
        cars[carIndex] = newCar;
        return {
          ...state,
          cars
        }
      }
    },
    deleteCar: (state, { payload}) => {
      const { carID } = payload;
      const carIndex = state.cars.findIndex((car) => car.userID === carID);
      if (carIndex !== 1) {
        const newCars = [...state.cars];
        newCars.splice(carIndex, 1)
        return ({
          ...state,
          cars: newCars
        })
      }
    },
  }
})

export const { 
  updateUsers, 
  updateUser, 
  deleteUser, 
  updateTrips, 
  updateBillings,
  updateCars,
  updateCar,
  deleteCar
} = adminSlice.actions;
export const getAllUsers = (state) => state.admin.users;
export const getAllCars = (state) => state.admin.cars;
export const getAllTrips = (state) => state.admin.trips;
export const getAllBilllings = (state) => state.admin.billings;

export default adminSlice.reducer;