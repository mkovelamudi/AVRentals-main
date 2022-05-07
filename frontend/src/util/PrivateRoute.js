import { useSelector } from "react-redux";
import { Redirect, Route } from "react-router";
import { isUserLoggedIn } from "../features/auth/AuthSlice";

export const PrivateRoute = ({ children, ...rest }) => {
  const isLoggedIn = useSelector(isUserLoggedIn);
  return (
    <Route
      {...rest}
      render={() => {
        return isLoggedIn === true ? children : <Redirect to="/" />;
      }}
    />
  );
};
