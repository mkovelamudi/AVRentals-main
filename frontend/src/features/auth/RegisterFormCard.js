import React, { useState, useCallback } from "react";
import { Card, Form, Button } from "react-bootstrap";
import { useDispatch } from "react-redux";

import { validateEmail, validatePassword } from "../../util/Validations";
import { registerAction } from "../../app/actions";




export const RegisterFormCard = ({setShowLogin, submitButtonText, showLoginText, onSuccessCB}) => {
  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [phone, setPhone] = useState("");
  const [role, setRole] = useState("user");
  const [password, setPassword] = useState("");
  const [address, setAddress] = useState("");

  const [isEmailValid, setIsEmailValid] = useState(true);
  const [isPasswordValid, setIsPasswordValid] = useState(true);

  const dispatch = useDispatch();

  const registerClicked = useCallback(
    (e) => {
      e.preventDefault();
      dispatch(registerAction({ email, password, name, phone, role, address }));
      if (onSuccessCB) 
        onSuccessCB();
    },
    [dispatch, email, password, name, phone, role, address, onSuccessCB]
  );

  return (
    <Card.Body>
      <Form
        style={{ width: "100%" }}
        onSubmit={(e) => {
          e.preventDefault();
          setIsEmailValid(validateEmail(email));
          setIsPasswordValid(validatePassword(password));
          if (!validateEmail(email) || !validatePassword(password)) {
            return;
          }
          registerClicked(e);
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
          >
            <option value="user">User</option>
            <option value="owner">Car Owner</option>
            <option value="admin">Administrator</option>
          </Form.Select>
        </Form.Group>
        <Form.Group className="mb-3" controlId="formBasicEmail">
          <Form.Label>Email address</Form.Label>
          <Form.Control
            type="email"
            value={email}
            placeholder="Enter email"
            onChange={(e) => setEmail(e.target.value)}
          />
          {!isEmailValid ? (
            <p style={{ color: "red", fontSize: "0.8rem" }}>Invalid Email</p>
          ) : null}
        </Form.Group>

        <Form.Group className="mb-3" controlId="formBasicPassword">
          <Form.Label>Password</Form.Label>
          <Form.Control
            type="password"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          {!isPasswordValid ? (
            <p style={{ color: "red", fontSize: "0.8rem" }}>
              Invalid Password
            </p>
          ) : null}
        </Form.Group>
        <Form.Group className="mb-3">
          <Button
            variant="info"
            type="submit"
            style={{ alignSelf: "center" }}
          >
            {submitButtonText}
          </Button>
        </Form.Group>

        <Form.Group className="mb-3">
          <Button
            variant="secondary"
            style={{ alignSelf: "center" }}
            onClick={(e) => {
              e.preventDefault();
              setShowLogin(true);
            }}
          >
            {showLoginText}
          </Button>
        </Form.Group>
      </Form>
    </Card.Body>
  );
};