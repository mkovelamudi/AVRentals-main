import {
  addUserCarAction,
  deleteUserCarAction,
  fetchOwnerCarsAction,
  findRideAction,
} from "../../app/actions";
import { call, takeEvery, put, select } from "@redux-saga/core/effects";
import { deleteApi, get, post } from "../../app/Api";
import { addAvailableCar, addUserCars } from "./HomeSlice";
import { getUserId } from "../auth/AuthSlice";

function* findRides({ payload }) {
  const response = yield call(post, {
    endpoint: "car/getCar",
    body: { current_location: payload.pickup.waypoint },
  });
  if (response.status) {
    yield put(addAvailableCar({ carAvailable: response.data[0] }));
  }
}
function* getAllUserCars() {
  const userId = yield select(getUserId);
  const response = yield call(get, { endpoint: `car/mycars/${userId}` });
  if (response.status) {
    yield put(addUserCars({ userCars: response.data }));
  }
}
function* addUserCar({ payload }) {
  console.log({ payload });
  const userId = yield select(getUserId);
  const response = yield call(post, {
    endpoint: "car/add",
    body: {
      manufacture: payload.manufacturer,
      model: payload.model,
      registration_number: payload.regNo,
      userID: userId,
    },
  });
  if (response.status) {
    yield put(fetchOwnerCarsAction());
  }
}
function* deleteUserCar({ payload }) {
  const response = yield call(deleteApi, {
    endpoint: `car/${payload.carID}`,
  });
  if (response.status) {
    yield put(fetchOwnerCarsAction());
  }
}
export function* findRidesSaga() {
  yield takeEvery(findRideAction.toString(), findRides);
}
export function* fetchOwnerCarsSaga() {
  yield takeEvery(fetchOwnerCarsAction.toString(), getAllUserCars);
}
export function* addUserCarSaga() {
  yield takeEvery(addUserCarAction.toString(), addUserCar);
}
export function* deleteUserCarSaga() {
  yield takeEvery(deleteUserCarAction.toString(), deleteUserCar);
}
