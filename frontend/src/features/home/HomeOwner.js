import React, { useEffect } from "react";
import { Accordion, Card, Col, Container, Button } from "react-bootstrap";
import { useDispatch, useSelector } from "react-redux";
import { fetchOwnerCarsAction } from "../../app/actions";
import logoImage from "../../assets/logo.jpg";
import { getUserCars } from "./HomeSlice";
import { AddCarForm } from "./AddCarForm";

export function HomeOwner() {
  const userCars = useSelector(getUserCars);

  const dispatch = useDispatch();

  useEffect(() => {
    dispatch(fetchOwnerCarsAction());
  }, [dispatch]);

  
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
          <AddCarForm />
        </Card>
      </Col>

      <Col>
        <Accordion>
          {userCars &&
            userCars.map((item, index) => {
              return (
                <Accordion.Item eventKey={index} key={index}>
                  <Accordion.Header>
                    <div
                      style={{
                        display: "flex",
                        flex: 1,
                        justifyContent: "space-between",
                      }}
                    >
                      <h5 className="lead" style={{ color: "blue" }}>
                        Car: {item.registration_number}
                      </h5>
                      <h6
                        className="lead"
                        style={{ color: "green", marginRight: "1vw" }}
                      >
                        Revenue: $
                        {item.Revenue ? item.Revenue.toFixed(2) : "0.00"}
                      </h6>
                    </div>
                  </Accordion.Header>
                  <Accordion.Body>
                    <div
                      style={{
                        display: "flex",
                        flex: 1,
                        justifyContent: "space-between",
                      }}
                    >
                      <div style={{ fontFamily: "sans-serif" }}>
                        <h4 className="lead">
                          {" "}
                          <small className="text-muted">
                            {" "}
                            {item.manufacture}
                          </small>{" "}
                          {item.model}
                        </h4>
                        <h4 className="lead">
                          <small className="text-muted">
                            {" "}
                            Total Trips Completed:
                          </small>{" "}
                          {item.Total_Trips ? item.Total_Trips : 0}
                        </h4>
                      </div>
                      <div>
                        <Button variant="danger" type="submit">
                          Remove
                        </Button>
                      </div>
                    </div>
                  </Accordion.Body>
                </Accordion.Item>
              );
            })}
        </Accordion>
      </Col>
    </Container>
  );
}
