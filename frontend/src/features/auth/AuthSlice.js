import { createSlice } from "@reduxjs/toolkit";

const initialState = {
  isLoggedIn: false,
};

export const authSlice = createSlice({
  name: "auth",
  initialState,
  reducers: {
    login: (state, { payload }) => ({ ...payload, ...{ isLoggedIn: true } }),
    logout: (state, action) => ({ isLoggedIn: false }),
    register: (state, { payload }) => ({ ...payload, ...{ isLoggedIn: true } }),
  },
});
export const { login, logout, register } = authSlice.actions;

export const isUserLoggedIn = (state) => state.auth.isLoggedIn;
export const getUserDetails = (state) => state.auth;
export const getUserId = (state) => state.auth.userID;

export default authSlice.reducer;
