import React, { useEffect } from "react";
import { Container, Nav, Navbar, Button, Form } from "react-bootstrap";
import { useSelector, useDispatch } from "react-redux";
import { logoutAction } from "../app/actions";
import { isUserLoggedIn } from "../features/auth/AuthSlice";
import { getUserDetails } from "../features/auth/AuthSlice";
import { useLocation } from "react-router-dom";

export function NavBar() {
  const isLoggedIn = useSelector(isUserLoggedIn);
  const userData = useSelector(getUserDetails);
  const dispatch = useDispatch();
  const location = useLocation();

  const logoutClicked = (e) => {
    e.preventDefault();
    dispatch(logoutAction());
  };

  if (isLoggedIn) {
    return (
      <Navbar
        className="me-auto my-2 my-lg-0"
        sticky="top"
        bg="dark"
        expand="lg"
        variant="dark"
      >
        <Container fluid>
          <Navbar.Brand href="/">
            {" "}
            <h4>
              <em>AV</em>Rentals
            </h4>
          </Navbar.Brand>
          <Navbar.Toggle aria-controls="basic-navbar-nav" />
          <Navbar.Collapse id="basic-navbar-nav">
            <Nav
              className="me-auto"
              defaultActiveKey="/home"
              activeKey={location.pathname}
            >
              <Nav.Link href="/home">
                <p
                  style={{
                    margin: 0,
                    fontStyle: "italic",
                  }}
                >
                  Home
                </p>
              </Nav.Link>
              {userData?.role === "user" && (
                <Nav.Link href="/home/bookings">
                  <p
                    style={{
                      margin: 0,
                      fontStyle: "italic",
                    }}
                  >
                    Bookings
                  </p>
                </Nav.Link>
              )}
              <Nav.Link href="/profile">
                <p
                  style={{
                    margin: 0,
                    fontStyle: "italic",
                  }}
                >
                  Profile
                </p>
              </Nav.Link>
            </Nav>
            <Form className="d-flex">
              <Button onClick={logoutClicked} variant="outline-light">
                Logout
              </Button>
            </Form>
          </Navbar.Collapse>
        </Container>
      </Navbar>
    );
  }
  return null;
}
