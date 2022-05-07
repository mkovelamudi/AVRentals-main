import { call, takeEvery, put } from "@redux-saga/core/effects";
import { get, put as putRequest, deleteApi } from "../../app/Api";
import { 
  deleteUserAction, 
  loadAllUsersAction, 
  updateUserAction, 
  loadAllTripsAction, 
  loadAllBillingsAction,
  loadAllCarsAction,
  updateCarAction,
  deleteCarAction
 } from "../../app/actions";
import { 
  updateUsers, 
  updateUser, 
  deleteUser, 
  updateTrips, 
  updateBillings,
  updateCars,
  updateCar,
  deleteCar
} from "./AdminSlice";



export function* loadAllUsersSaga() {
  yield takeEvery(loadAllUsersAction.toString(), loadAllUsers);
}

export function* updateUserSaga() {
  yield takeEvery(updateUserAction.toString(), editUser)
}

export function* deleteUserSaga() {
  yield takeEvery(deleteUserAction.toString(), removeUser)
}

export function* loadAllCarsSaga() {
  yield takeEvery(loadAllCarsAction.toString(), loadAllCars);
}

export function* updateCarSaga() {
  yield takeEvery(updateCarAction.toString(), editCar)
}

export function* deleteCarSaga() {
  yield takeEvery(deleteCarAction.toString(), removeCar)
}

export function * loadAllTripsSaga() {
  yield takeEvery(loadAllTripsAction.toString(), loadAllTrips);
}

export function* loadAllBillingsSaga() {
  yield takeEvery(loadAllBillingsAction.toString(), loadAllBillings);
}


function *loadAllUsers() {
  const response = yield call(get, {
    endpoint: '',
  });
  if (response.status) {
    yield put(updateUsers({ users: response.data }));
    
  }
}

function* editUser({payload}) {
  const response = yield call(putRequest, {
    endpoint: `editprofile/${payload.userID}`,
    body: { ...payload }
  });
  if (response.status) {
    const getResponse = yield call(get, {
      endpoint: payload.userID,
    });
    if (getResponse.status) {
      yield put(updateUser({user: getResponse.data}))
    }
  }
}

function* removeUser({payload}) {

  const { userID } = payload;
  const response = yield call(deleteApi, {
    endpoint: userID
  });
  if (response.status) {
    yield put(deleteUser({userID}));
  }
}


function *loadAllCars() {
  const response = yield call(get, {
    endpoint: 'car/all',
  });
  if (response.status) {
    yield put(updateCars({ cars: response.data }));
    
  }
}

function* editCar({payload}) {
  const response = yield call(putRequest, {
    endpoint: `car/edit/${payload.carID}`,
    body: { ...payload }
  });
  if (response.status) {
    const getResponse = yield call(get, {
      endpoint: `car/${payload.carID}`,
    });
    if (getResponse.status) {
      yield put(updateCar({car: getResponse.data}))
    }
  }
}

function* removeCar({payload}) {
  const { carID } = payload;
  const response = yield call(deleteApi, {
    endpoint: `car/${carID}`
  });
  console.log('remove car response: ', response)
  if (response.status) {
    yield put(deleteCar({carID}));
  }
}

function* loadAllTrips() {
  const response = yield call(get, {
    endpoint: 'trip/all'
  })


  if (response.status) {
    yield put(updateTrips({trips: response.data}))
  }
}

function* loadAllBillings() {
  const response = yield call(get, {
    endpoint: 'billing/all'
  })


  if (response.status) {
    yield put(updateBillings({billings: response.data}))
  }
}