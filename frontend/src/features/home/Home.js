import React, { useCallback, useState } from "react";
import {
  Card,
  Container,
  Form,
  Button,
  Image,
  Col,
  Modal,
  Spinner,
  Table,
} from "react-bootstrap";
import { useDispatch, useSelector } from "react-redux";
import logoImage from "../../assets/logo.jpg";
import { addAvailableCar, getAvailableCar, getLocations } from "./HomeSlice";
import { StaticGoogleMap, Marker } from "react-static-google-map";
import { getUserDetails } from "../auth/AuthSlice";
import { Redirect, useHistory } from "react-router";
import {
  bookRideAction,
  findRideAction,
  getAllBookingsAction,
} from "../../app/actions";
import moment from "moment";

export function Home() {
  const locations = useSelector(getLocations);
  const userData = useSelector(getUserDetails);
  const [pickup, setPickup] = useState(0);
  const [drop, setDrop] = useState(1);
  const [showSearchModal, setShowSearchModal] = useState(false);
  const carsAvailableForRide = useSelector(getAvailableCar);
  const dispatch = useDispatch();
  const history = useHistory();
  const closeSearchModal = () => setShowSearchModal(false);
  const openSearchModal = () => setShowSearchModal(true);

  const searchForRides = () => {
    openSearchModal();
    dispatch(
      findRideAction({ pickup: locations[pickup], drop: locations[drop] })
    );
    dispatch(getAllBookingsAction());
  };

  const bookRide = useCallback(
    (e) => {
      e.preventDefault();
      dispatch(
        bookRideAction({
          pickup_location: locations[pickup].waypoint,
          dropoff_location: locations[drop].waypoint,
          carID: carsAvailableForRide.carID,
          userID: userData.userID,
          start_time: moment().format("YYYY-MM-DD HH:mm:ss"),
        })
      );
      closeSearchModal();
      dispatch(addAvailableCar({ carAvailable: null }));
      history.push("/home/bookings");
    },
    [dispatch, pickup, drop, history, locations, carsAvailableForRide, userData]
  );
  const rideBookingForm = () => {
    return (
      <Card.Body>
        <Form style={{ width: "100%" }}>
          <Form.Group className="mb-3" controlId="formBasicEmail">
            <Form.Label style={{ color: "green" }}>
              Choose Pick-up Location
            </Form.Label>
            <Form.Select
              size="sm"
              value={pickup}
              onChange={(e) => {
                e.preventDefault();
                setPickup(e.target.value);
              }}
            >
              {locations.map((item, index) => {
                return (
                  <option key={index} value={index}>
                    {item.name}
                  </option>
                );
              })}
            </Form.Select>
          </Form.Group>

          <Form.Group className="mb-3" controlId="formBasicEmail">
            <Form.Label style={{ color: "red" }}>
              Choose Drop-off Location
            </Form.Label>
            <Form.Select
              size="sm"
              value={drop}
              onChange={(e) => {
                console.log(e.target.value);
                e.preventDefault();
                setDrop(e.target.value);
              }}
            >
              {locations.map((item, index) => {
                return (
                  <option key={index} value={index}>
                    {item.name}
                  </option>
                );
              })}
            </Form.Select>
          </Form.Group>

          <Form.Group className="mb-3">
            <Button
              variant="info"
              type="submit"
              style={{ alignSelf: "center" }}
              onClick={(e) => {
                e.preventDefault();
                console.log("HERe-----", { pickup, drop });
                if (pickup === drop) {
                  alert("Pickup and Drop locations must be different");
                } else {
                  searchForRides();
                }
              }}
            >
              Find Ride
            </Button>
          </Form.Group>
        </Form>
      </Card.Body>
    );
  };

  const renderMap = () => {
    return (
      <StaticGoogleMap
        as={(props) => (
          <Image
            {...props}
            style={{
              flex: 1,
              height: "92vh",
              width: "100%",
            }}
            fluid
          />
        )}
        scale="2"
        size="750x400"
        className="img-fluid"
        apiKey={process.env.REACT_APP_GOOGLE_MAPS_API}
        zoom={14}
        center="37.32948626822242,-121.8761059820004"
        format="jpg"
        mapId="e620ed351bce9b09"
      >
        <Marker
          size="small"
          location={locations[pickup].location}
          color="green"
          label="S"
        />
        <Marker
          size="small"
          location={locations[drop].location}
          color="red"
          label="S"
        />
      </StaticGoogleMap>
    );
  };

  if (userData.role === "owner") {
    return (
      <>
        <Redirect to="/home/owner" />
      </>
    );
  }
  if (userData.role === "admin") {
    return (
      <>
        <Redirect to="/home/admin" />
      </>
    );
  }
  return (
    <Container
      fluid
      style={{
        height: "92vh",
        width: "100vw",
        overflow: "hidden",
        margin: 0,
        padding: 0,
        display: "flex",
      }}
    >
      <Col xs={3}>
        {" "}
        <Card
          style={{
            height: "100vh",
            alignSelf: "center",
            opacity: 0.9,
          }}
        >
          <Card.Img
            variant="top"
            src={logoImage}
            style={{
              width: "6vw",
              height: "12vh",
              alignSelf: "center",
            }}
          />
          {rideBookingForm()}
        </Card>
      </Col>
      <Col>{renderMap()}</Col>

      <Modal show={showSearchModal} onHide={closeSearchModal} centered>
        <Modal.Header>
          <Modal.Title>Searching for nearby cars</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          {!carsAvailableForRide ? (
            <Spinner
              style={{ marginLeft: "45%" }}
              animation="border"
              variant="success"
            />
          ) : (
            <Table striped bordered hover>
              <tbody>
                <tr>
                  <td>{carsAvailableForRide.manufacture}</td>
                  <td>{carsAvailableForRide.model}</td>
                  <td>{carsAvailableForRide.registration_number}</td>
                </tr>
              </tbody>
            </Table>
          )}
        </Modal.Body>
        <Modal.Footer>
          <Button
            variant={"secondary"}
            onClick={(e) => {
              e.preventDefault();
              closeSearchModal();
            }}
          >
            Cancel Search
          </Button>
          {carsAvailableForRide && (
            <Button variant={"primary"} onClick={bookRide}>
              Book Ride
            </Button>
          )}
        </Modal.Footer>
      </Modal>
    </Container>
  );
}
