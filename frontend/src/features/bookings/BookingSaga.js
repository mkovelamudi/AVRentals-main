import { call, takeEvery, put, select, delay } from "@redux-saga/core/effects";
import {
  bookRideAction,
  getAllBookingsAction,
  startRideAction,
  trackRideAction,
} from "../../app/actions";
import { get, post } from "../../app/Api";
import { getUserId } from "../auth/AuthSlice";
import { updateCurrentTripId, updatePrevRides } from "./BookingSlice";

//Watcher Sagas
export function* bookRidesSaga() {
  yield takeEvery(bookRideAction.toString(), bookRide);
}
export function* getAllUserBookingsSaga() {
  yield takeEvery(getAllBookingsAction.toString(), getAllUserBookings);
}
export function* tripTrackingSaga() {
  yield takeEvery(trackRideAction.toString(), trackRide);
}

export function* tripStartSaga() {
  yield takeEvery(startRideAction.toString(), startRide);
}

function* bookRide({ payload }) {
  const response = yield call(post, {
    endpoint: "trip/add",
    body: { ...payload },
  });
  if (response.status) {
    yield put(updateCurrentTripId({ currentTripId: response.data.insertId }));
    yield put(getAllBookingsAction());
    yield put(trackRideAction());
  }
}

function* trackRide() {
  while (true) {
    const userId = yield select(getUserId);
    yield delay(1000);
    const response = yield call(get, {
      endpoint: `trip/mytrips/${userId}`,
    });
    if (response.status) {
      yield put(updatePrevRides({ prevRides: response.data }));
      if (
        response?.data[0] &&
        (response.data[0].iscompleted || response.data[0].collision)
      )
        break;
    }
  }
}
function* getAllUserBookings() {
  const userId = yield select(getUserId);

  const response = yield call(get, {
    endpoint: `trip/mytrips/${userId}`,
  });
  if (response.status) {
    let prevRides = [...response.data];
    prevRides.sort((a, b) => a.tripID > b.tripID);
    yield put(updatePrevRides({ prevRides: response.data }));
    yield put(updateCurrentTripId({ currentTripId: prevRides[0].tripID }));
  }
}

function* startRide({ payload }) {
  const response = yield call(post, {
    endpoint: "trip/pickedup",
    body: payload,
  });
  if (response.status) {
    console.log(response);
  }
}
