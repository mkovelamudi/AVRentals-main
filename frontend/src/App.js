import React from "react";
import { BrowserRouter, Route } from "react-router-dom";
import { AuthPage } from "./features/auth/AuthPage";
import { NavBar } from "./components/Navbar";
import { PrivateRoute } from "./util/PrivateRoute";
import { Home } from "./features/home/Home";
import { HomeAdmin } from "./features/home/HomeAdmin";
import { HomeOwner } from "./features/home/HomeOwner";
import { Bookings } from "./features/bookings/Bookings";
import { Profile } from "./features/profile/Profile";
function App() {
  return (
    <BrowserRouter>
      <NavBar />
      <Route exact path="/">
        <AuthPage />
      </Route>
      <PrivateRoute exact path="/home">
        <Home />
      </PrivateRoute>
      <PrivateRoute exact path="/home/admin">
        <HomeAdmin />
      </PrivateRoute>
      <PrivateRoute exact path="/home/owner">
        <HomeOwner />
      </PrivateRoute>
      <PrivateRoute exact path="/home/bookings">
        <Bookings />
      </PrivateRoute>
      <PrivateRoute exact path="/profile">
        <Profile />
      </PrivateRoute>
    </BrowserRouter>
  );
}

export default App;
