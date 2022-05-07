import { createSlice } from "@reduxjs/toolkit";

const initialState = {
  locations: [
    {
      name: "MLK Library",
      location: "37.33573710227817,-121.88502026030886",
      waypoint: "l1",
    },
    {
      name: "San Pedro",
      location: "37.33665012618762,-121.89432303147323",
      waypoint: "l2",
    },
    {
      name: "1st & Santa Clara",
      location: "37.335964897149935,-121.8902156007891",
      waypoint: "l3",
    },
    {
      name: "City Hall",
      location: "37.33805588371879,-121.88483041428249",
      waypoint: "l4",
    },
    {
      name: "CEFCU Stadium",
      location: "37.319838125094286,-121.86823182962517",
      waypoint: "l5",
    },
    {
      name: "Happy Hollow Park",
      location: "37.326205514037355,-121.86153097443075",
      waypoint: "l6",
    },
    {
      name: "Adobe Headquater",
      location: "37.3309709178039,-121.89388241387533",
      waypoint: "l7",
    },
    {
      name: "Notre Damne High School",
      location: "37.328671176546266, -121.88258045937138",
      waypoint: "l8",
    },
  ],
  userCars: null,
  carAvailable: null,
};

export const homeSlice = createSlice({
  name: "home",
  initialState,
  reducers: {
    addUserCars: (state, { payload }) => ({
      ...state,
      ...{ userCars: payload.userCars },
    }),
    addAvailableCar: (state, { payload }) => ({ ...state, ...payload }),
  },
});

export const { addUserCars, addAvailableCar } = homeSlice.actions;

export const getLocations = (state) => state.home.locations;
export const getUserCars = (state) => state.home.userCars;
export const getAvailableCar = (state) => state.home.carAvailable;

export default homeSlice.reducer;
