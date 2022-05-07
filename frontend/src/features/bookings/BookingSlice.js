import { createSlice } from "@reduxjs/toolkit";

const initialState = {
  prevRides: [],
  currentTripId: null,
};

export const bookingSlice = createSlice({
  name: "ride",
  initialState,
  reducers: {
    updateCurrentTripId: (state, { payload }) => ({
      ...state,
      ...{ currentTripId: payload.currentTripId },
    }),
    updatePrevRides: (state, { payload }) => ({
      ...state,
      ...{ prevRides: payload.prevRides },
    }),
  },
});

export const { updateCurrentTripId, updatePrevRides } = bookingSlice.actions;
export const getCurrentTripId = (state) => state.ride.currentTripId;
export const getPrevRides = (state) => state.ride.prevRides;

export default bookingSlice.reducer;
