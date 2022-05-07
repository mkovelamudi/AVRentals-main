import { call, takeEvery, put, select } from "@redux-saga/core/effects";
import {
  fetchUserProfileAction,
  loginAction,
  logoutAction,
  registerAction,
  updateUserProfileAction,
} from "../../app/actions";
import { get, post, put as apiPut } from "../../app/Api";
import {
  getUserDetails,
  getUserId,
  login,
  logout,
  register,
} from "./AuthSlice";

function* loginUser({ payload }) {
  const email = payload.email;
  const password = payload.password;
  const response = yield call(post, {
    endpoint: "login",
    body: {
      password: password,
      user_email: email,
    },
  });
  if (response.status) {
    yield put(login(response.data[0]));
  } else {
    alert(response.message);
  }
}

function* logoutUser() {
  yield put(logout());
}

function* registerUser({ payload }) {
  const reqBody = {
    password: payload.password,
    user_email: payload.email,
    firstname: payload.name.split(" ")[0],
    lastname:
      payload.name.split(" ").length > 1 ? payload.name.split(" ")[1] : "",
    address: payload.address,
    phone: payload.phone,
    role: payload.role,
  };
  const response = yield call(post, {
    endpoint: "register",
    body: reqBody,
  });
  console.log(response);
  if (response.status) {
    reqBody.userID = response.data.insertId;
    yield put(register(reqBody));
  }
}

function* updateProfile({ payload }) {
  const userId = yield select(getUserId);
  const userProfile = yield select(getUserDetails);
  const response = yield call(apiPut, {
    endpoint: `editprofile/${userId}`,
    body: {
      ...userProfile,
      ...payload,
      ...{
        firstname: payload.name.split(" ")[0],
        lastname:
          payload.name.split(" ").length > 1 ? payload.name.split(" ")[1] : "",
      },
    },
  });
  if (response.status) {
    yield put(fetchUserProfileAction());
    alert("Profile Updated Successfully");
  } else {
    alert(response.message);
  }
}

function* fetchUserProfile() {
  const userId = yield select(getUserId);
  const response = yield call(get, { endpoint: `${userId}` });
  if (response.status) {
    yield put(login(response.data[0]));
  }
}

export function* loginSaga() {
  yield takeEvery(loginAction.toString(), loginUser);
}

export function* registerSaga() {
  yield takeEvery(registerAction.toString(), registerUser);
}

export function* logoutSaga() {
  yield takeEvery(logoutAction.toString(), logoutUser);
}

export function* updateProfileSaga() {
  yield takeEvery(updateUserProfileAction.toString(), updateProfile);
}

export function* fetchUserProfileSaga() {
  yield takeEvery(fetchUserProfileAction.toString(), fetchUserProfile);
}
