import React, { useCallback, useEffect, useState } from "react";
import {
  Card,
  Col,
  Container,
  ProgressBar,
  Row,
  Spinner,
  Button,
  Image,
} from "react-bootstrap";
import { useDispatch, useSelector } from "react-redux";
import { getLocations } from "../home/HomeSlice";
import logoImage from "../../assets/logo.jpg";
import tripCompleteImage from "../../assets/complete.png";
import tripCollisionImage from "../../assets/crash.png";
import { startRideAction } from "../../app/actions";
import { PDFDownloadLink } from "@react-pdf/renderer";
import { PdfInvoice } from "./Invoice";
import { getUserDetails } from "../auth/AuthSlice";

export function Trip({ tripData }) {
  const dispatch = useDispatch();
  const locations = useSelector(getLocations);
  const user = useSelector(getUserDetails);
  const [maxEta, setMaxEta] = useState(2);
  const [tripID, setTripID] = useState(tripData.tripID);
  const [crash, setCrash] = useState(false);

  useEffect(() => {
    if (tripData?.isPickedUp && tripData?.eta > maxEta) {
      setMaxEta(tripData?.eta);
    }
    setTripID(tripData.tripID);
  }, [tripData, maxEta]);

  const getInvoiceDocument = useCallback(() => {
    console.log("from trip.js", tripData);
    return <PdfInvoice tripData={{ ...tripData }} user={user} />;
  }, [tripData]);

  const startTrip = useCallback(
    (e) => {
      e.preventDefault();
      dispatch(startRideAction({ tripID, crash }));
    },
    [dispatch, tripID, crash]
  );
  if (!tripID) {
    return <div>No Trip Found</div>;
  }

  const renderTripProgress = () => {
    if (tripData.iscompleted) {
      return (
        <Col
          style={{
            display: "flex",
            flex: 1,
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <Image
            src={tripCompleteImage}
            style={{
              width: "4vw",
              height: "8vh",
              alignSelf: "center",
              marginBottom: "1vh",
            }}
          />

          <h6>Trip Complete</h6>
        </Col>
      );
    }
    if (tripData.collision) {
      return (
        <Col
          style={{
            display: "flex",
            flex: 1,
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <Image
            src={tripCollisionImage}
            style={{
              width: "4vw",
              height: "8vh",
              alignSelf: "center",
              marginBottom: "1vh",
            }}
          />

          <h6>Collision Reported</h6>
        </Col>
      );
    }

    if (!tripData?.isPickedUp) {
      return (
        <Col
          style={{
            display: "flex",
            flex: 1,
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <Button
            variant="primary"
            style={{ alignSelf: "center" }}
            onClick={startTrip}
          >
            Start Trip
          </Button>
          <div style={{ margin: "2px" }}>
            <input
              type="checkbox"
              id="scales"
              name="scales"
              value={crash}
              onChange={(e) => setCrash(e.target.checked)}
            />
            <label htmlFor="scales" style={{ margin: "3px" }}>
              Crash
            </label>
          </div>
        </Col>
      );
    } else {
      return (
        <Col
          style={{
            justifyContent: "center",
            alignSelf: "center",
          }}
        >
          <ProgressBar min={0} max={maxEta} now={maxEta - tripData?.eta} />
        </Col>
      );
    }
  };
  const renderTripData = () => {
    const returnTripBillorCollision = () => {
      if (tripData?.iscompleted === 1) {
        return (
          <>
            <div>
              <div
                style={{
                  width: "100%",
                  height: "2.5vh",
                  borderBottom: "1px solid black",
                  textAlign: "center",
                  marginBottom: "2vh",
                }}
              >
                <span
                  style={{
                    backgroundColor: "white",
                    fontSize: "1.3rem",
                  }}
                >
                  Cost Details
                </span>
              </div>
              <Row>
                <Container
                  style={{
                    width: "100%",
                    height: "100%",
                    display: "flex",
                    flexDirection: "column",
                    justifyContent: "flex-end",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                    }}
                  >
                    <div>
                      <p>Trip Cost</p>
                    </div>
                    <div>
                      <p>$ {tripData?.cost.toFixed(2)}</p>
                    </div>
                  </div>
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                    }}
                  >
                    <div>
                      <p>Tax</p>
                    </div>
                    <div>
                      <p>$ {tripData?.tax.toFixed(2)}</p>
                    </div>
                  </div>
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                    }}
                  >
                    <div>
                      <p>Amount</p>
                    </div>
                    <div>
                      <p>$ {tripData?.total_cost.toFixed(2)}</p>
                    </div>
                  </div>
                  <div style={{ display: "flex", justifyContent: "center" }}>
                    <Button variant="outline-info">
                      {""}
                      <PdfInvoice tripData={tripData} user={user} />
                    </Button>
                  </div>
                </Container>
              </Row>
            </div>
          </>
        );
      } else if (tripData?.collision) {
        return (
          <div
            style={{
              display: "flex",
              flexDirection: "row",
              justifyContent: "center",
              alignContent: "center",
              width: "100%",
            }}
          >
            <p
              className="display-6"
              style={{ fontSize: "1.5rem", color: "red" }}
            >
              Collision Reported. Sending Emergency Services
            </p>
          </div>
        );
      } else {
        return (
          <div
            style={{
              display: "flex",
              flexDirection: "row",
              justifyContent: "center",
              alignContent: "center",
              width: "100%",
            }}
          >
            <p
              className="display-6"
              style={{ fontSize: "1.5rem", color: "red" }}
            >
              Bill will be genrated after the trip
            </p>
          </div>
        );
      }
    };
    return (
      <Card
        style={{
          height: "80vh",
          width: "60vw",
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
        <Card.Body
          style={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "space-around",
          }}
        >
          <div style={{ display: "flex", flexDirection: "row" }}>
            <Col
              xs={4}
              style={{
                display: "flex",
                flex: 1,
                flexDirection: "column",
                justifyContent: "center",
                alignItems: "flex-start",
              }}
            >
              <p className="display-6">Pickup</p>
              <h5 style={{ color: "green" }}>
                {
                  locations.filter(
                    (item) => item.waypoint === tripData.pickup_location
                  )[0].name
                }
              </h5>
            </Col>

            {tripData.collision ||
            tripData.iscompleted ||
            tripData?.atPickUp ? (
              renderTripProgress()
            ) : (
              <Col
                style={{
                  display: "flex",
                  flex: 1,
                  flexDirection: "column",
                  justifyContent: "center",
                  alignItems: "center",
                }}
              >
                <Spinner
                  style={{ marginBottom: "1vh" }}
                  animation="border"
                  variant="success"
                />

                <span>Waiting for car to arrive...</span>
              </Col>
            )}
            <Col
              xs={4}
              style={{
                display: "flex",
                flex: 1,
                flexDirection: "column",
                justifyContent: "center",
                alignItems: "flex-end",
              }}
            >
              <p className="display-6">Drop</p>
              <h5 style={{ color: "blue" }}>
                {
                  locations.filter(
                    (item) => item.waypoint === tripData.dropoff_location
                  )[0].name
                }
              </h5>
            </Col>
          </div>

          <div>
            <div
              style={{
                width: "100%",
                height: "2.5vh",
                borderBottom: "1px solid black",
                textAlign: "center",
                marginBottom: "2vh",
              }}
            >
              <span
                style={{
                  backgroundColor: "white",
                  fontSize: "1.3rem",
                }}
              >
                Car Details
              </span>
            </div>
            <Row>
              <Container
                style={{
                  width: "100%",
                  height: "100%",
                  display: "flex",
                  flexDirection: "column",
                  justifyContent: "flex-end",
                }}
              >
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                  }}
                >
                  <div>
                    <p>Brand</p>
                  </div>
                  <div>
                    <p>{tripData?.manufacture}</p>
                  </div>
                </div>
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                  }}
                >
                  <div>
                    <p>Car</p>
                  </div>
                  <div>
                    <p>{tripData?.model}</p>
                  </div>
                </div>
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                  }}
                >
                  <div>
                    <p>Reg No</p>
                  </div>
                  <div>
                    <p>{tripData?.registration_number}</p>
                  </div>
                </div>
              </Container>
            </Row>
          </div>
          {returnTripBillorCollision()}
        </Card.Body>
      </Card>
    );
  };
  return (
    <Container
      fluid
      style={{
        height: "90vh",
        width: "40vw",
        display: "flex",
        justifyContent: "center",
        alignContent: "center",
      }}
    >
      {tripData ? (
        renderTripData()
      ) : (
        <Spinner
          style={{ marginLeft: "45%" }}
          animation="border"
          variant="success"
        />
      )}
    </Container>
  );
}
