import { configureStore } from "@reduxjs/toolkit";
import createSagaMiddleware from "redux-saga";
import authReducer from "../features/auth/AuthSlice";
import homeReducer from "../features/home/HomeSlice";
import ridesReducer from "../features/bookings/BookingSlice";
import adminReducer from "../features/admin/AdminSlice";
import { persistReducer } from "redux-persist";
import storage from "redux-persist/lib/storage";
import { combineReducers } from "redux";
import rootSaga from "./saga";
import { logoutAction } from "./actions";

const persistConfig = {
  key: "root",
  storage,
};

const appReducer = combineReducers({
  auth: authReducer,
  home: homeReducer,
  ride: ridesReducer,
  admin: adminReducer,
});
const rootReducer = (state, action) => {
  if (action.type === logoutAction.type) {
    return appReducer(undefined, action);
  }
  return appReducer(state, action);
};
const persistedReducer = persistReducer(persistConfig, rootReducer);

const sagaMiddleware = createSagaMiddleware();

export const store = configureStore({
  reducer: persistedReducer,
  middleware: [sagaMiddleware],
});

sagaMiddleware.run(rootSaga);
