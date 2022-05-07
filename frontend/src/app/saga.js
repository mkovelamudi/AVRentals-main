import { all, spawn } from "redux-saga/effects";
import {
  fetchUserProfileSaga,
  loginSaga,
  logoutSaga,
  registerSaga,
  updateProfileSaga,
} from "../features/auth/AuthSaga";
import {
  addUserCarSaga,
  deleteUserCarSaga,
  fetchOwnerCarsSaga,
  findRidesSaga,
} from "../features/home/HomeSaga";
import {
  bookRidesSaga,
  getAllUserBookingsSaga,
  tripStartSaga,
  tripTrackingSaga,
} from "../features/bookings/BookingSaga";
import {
  deleteUserSaga,
  loadAllUsersSaga,
  updateUserSaga,
  loadAllTripsSaga,
  loadAllBillingsSaga,
  loadAllCarsSaga,
  updateCarSaga,
  deleteCarSaga,
} from "../features/admin/AdminSaga";

export default function* rootSaga() {
  yield all([
    spawn(loginSaga),
    spawn(logoutSaga),
    spawn(registerSaga),
    spawn(findRidesSaga),
    spawn(bookRidesSaga),
    spawn(tripTrackingSaga),
    spawn(tripStartSaga),
    spawn(fetchOwnerCarsSaga),
    spawn(addUserCarSaga),
    spawn(deleteUserCarSaga),
    spawn(getAllUserBookingsSaga),
    spawn(loadAllUsersSaga),
    spawn(updateUserSaga),
    spawn(deleteUserSaga),
    spawn(loadAllTripsSaga),
    spawn(loadAllBillingsSaga),
    spawn(loadAllCarsSaga),
    spawn(updateCarSaga),
    spawn(deleteCarSaga),
    spawn(updateProfileSaga),
    spawn(fetchUserProfileSaga),
  ]);
}
