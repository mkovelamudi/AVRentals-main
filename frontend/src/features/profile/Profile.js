import React, { useCallback, useEffect, useState } from "react";
import { Card, Container, Form, Button } from "react-bootstrap";
import { useDispatch, useSelector } from "react-redux";
import { getUserDetails } from "../auth/AuthSlice";
import homeImage from "../../assets/home.jpg";
import profileImage from "../../assets/profile.png";
import { updateUserProfileAction } from "../../app/actions";

export function Profile() {
  const userProfile = useSelector(getUserDetails);
  const [name, setName] = useState(
    `${userProfile.firstname} ${
      userProfile.lastname ? userProfile.lastname : ""
    }`
  );
  const [phone, setPhone] = useState(userProfile.phone);
  const [role, setRole] = useState(userProfile.role);
  const [address, setAddress] = useState(userProfile.address);
  const dispatch = useDispatch();

  const updateProfile = useCallback(
    (e) => {
      e.preventDefault();
      dispatch(updateUserProfileAction({ name, phone, address }));
    },
    [dispatch, name, phone, address]
  );

  const registerFormCard = () => {
    return (
      <Card.Body>
        <Form
          style={{ width: "100%" }}
          onSubmit={(e) => {
            e.preventDefault();
            updateProfile(e);
          }}
        >
          <Form.Group className="mb-3" controlId="formBasicName">
            <Form.Label>Name</Form.Label>
            <Form.Control
              required
              type="text"
              value={name}
              placeholder="Enter Name"
              onChange={(e) => setName(e.target.value)}
            />
          </Form.Group>
          <Form.Group className="mb-3" controlId="formBasicPhone">
            <Form.Label>Phone</Form.Label>
            <Form.Control
              required
              type="phone"
              value={phone}
              placeholder="Enter Phone"
              onChange={(e) => setPhone(e.target.value)}
            />
          </Form.Group>
          <Form.Group className="mb-3" controlId="formBasicPhone">
            <Form.Label>Address</Form.Label>
            <Form.Control
              required
              type="text"
              value={address}
              placeholder="Enter Address"
              onChange={(e) => setAddress(e.target.value)}
            />
          </Form.Group>
          <Form.Group className="mb-3" controlId="formBasicRole">
            <Form.Label>Role</Form.Label>
            <Form.Select
              size="sm"
              onChange={(e) => setRole(e.target.value)}
              value={role}
              disabled
            >
              <option value="user">User</option>
              <option value="owner">Car Owner</option>
              <option value="admin">Admin</option>
            </Form.Select>
          </Form.Group>

          <Form.Group className="mb-3">
            <Button
              variant="info"
              type="submit"
              style={{ alignSelf: "center" }}
            >
              Save Changes
            </Button>
          </Form.Group>
        </Form>
      </Card.Body>
    );
  };
  return (
    <Container
      fluid
      style={{
        backgroundImage: `url(${homeImage})`,
        height: "100vh",
        backgroundSize: "100vw 100vh",
        display: "flex",
        justifyContent: "center",
      }}
    >
      <Card
        style={{
          width: "30vw",
          height: "65%",
          alignSelf: "center",
          opacity: 0.9,
        }}
      >
        <Card.Img
          variant="top"
          src={profileImage}
          style={{
            width: "5vw",
            height: "12vh",
            alignSelf: "center",
          }}
        />
        <p
          style={{ fontSize: "1.5rem", alignSelf: "center", margin: 0 }}
          className="display-6"
        >
          Update Profile
        </p>
        {registerFormCard()}
      </Card>
    </Container>
  );
}
