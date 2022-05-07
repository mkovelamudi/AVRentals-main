import React, { useState } from 'react'
import { Col } from 'react-bootstrap'
import { Container, Nav } from 'react-bootstrap'

import { AdminBillings } from '../admin/AdminBillings'
import { AdminCars } from '../admin/AdminCars'
import { AdminTrips } from '../admin/AdminTrips'
import { AdminUsers } from '../admin/AdminUsers'

export function HomeAdmin () {
  const [ subPage, setSubPage ] = useState('users')

  const renderNavBar = () => {
    return (
      <Nav className='admin-nav-bar'>
        <Nav.Item>
          <Nav.Link className='admin-navigation-item' onClick={() => setSubPage('users')}>
            Users
          </Nav.Link>
        </Nav.Item>
        <Nav.Item>
          <Nav.Link className='admin-navigation-item' onClick={() => setSubPage('cars')}>
            Cars
          </Nav.Link>
        </Nav.Item>
        <Nav.Item>
          <Nav.Link className='admin-navigation-item' onClick={() => setSubPage('trips')}>
            Trips
          </Nav.Link>
        </Nav.Item>
        <Nav.Item>
          <Nav.Link className='admin-navigation-item' onClick={() => setSubPage('billings')}>
            Billings
          </Nav.Link>
        </Nav.Item>
      </Nav>
    )
  }

  const renderContent = subPage => {
    switch (subPage) {
      case 'trips':
        return <AdminTrips />
      case 'cars':
        return <AdminCars />
      case 'billings':
        return <AdminBillings />
      case 'users':
      default:
        return <AdminUsers />
    }
  }

  return (
    <Container className="admin-container">
      <Col xs={2}> {renderNavBar()} </Col>
      <Col> {renderContent(subPage)} </Col>
    </Container>
  )
}
