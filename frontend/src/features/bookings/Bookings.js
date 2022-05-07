import React, { useEffect, useState } from 'react'
import { Accordion, Col, Card, useAccordionButton } from 'react-bootstrap'
import { useDispatch, useSelector } from 'react-redux'
import { Trip } from '../trip/Trip'
import { getPrevRides } from './BookingSlice'
import moment from 'moment'
import { getAllBookingsAction } from '../../app/actions'

export function Bookings () {
  const bookings = useSelector(getPrevRides)
  const [selectedIndex, setSelectedindex] = useState(0)
  const dispatch = useDispatch()
  useEffect(() => {
    dispatch(getAllBookingsAction())
  }, [dispatch])

  function AccordianToggle ({ children, eventKey }) {
    const decoratedOnClick = useAccordionButton(eventKey, () =>
      setSelectedindex(eventKey)
    )

    return (
      <button
        onClick={decoratedOnClick}
        style={{
          background: 'transparent',
          border: 'none',
          color: 'blue',
          textDecoration: 'underline',
          fontStyle: 'italic'
        }}
      >
        {children}
      </button>
    )
  }

  if (bookings.length === 0) {
    return 'No Booking Found'
  }
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'row',
        height: '92vh',
        width: '100vw'
      }}
    >
      <Col style={{ height: '100%', overflowY: 'scroll', padding: '10px' }}>
        <Accordion defaultActiveKey={selectedIndex}>
          {bookings.map((item, index) => {
            return (
              <Card
                key={index}
                style={{
                  border: selectedIndex === index ? '2px solid green' : ''
                }}
              >
                <Card.Header>
                  <div
                    style={{
                      display: 'flex',
                      flex: 1,
                      justifyContent: 'space-between'
                    }}
                  >
                    <em>
                      <p className='diplay-6' style={{ margin: 0 }}>
                        {moment.utc(item.start_time).format("ddd, Do MMM 'YY")}
                      </p>
                      <p className='diplay-6' style={{ margin: 0 }}>
                        {moment.utc(item.start_time).format('LT')}
                      </p>
                    </em>
                    <div>
                      {item?.total_cost && (
                        <h6
                          className='display-6'
                          style={{
                            fontSize: '1.2rem',
                            margin: 0,
                            alignSelf: 'center'
                          }}
                        >
                          Cost: $ {item.total_cost.toFixed(2)}
                        </h6>
                      )}
                      {selectedIndex !== index && (
                        <AccordianToggle eventKey={index}>
                          Show Details
                        </AccordianToggle>
                      )}
                    </div>
                  </div>
                </Card.Header>
                <Accordion.Collapse eventKey={index}>
                  <Card.Body>
                    <div
                      style={{
                        display: 'flex',
                        flex: 1,
                        justifyContent: 'space-between'
                      }}
                    >
                      <div style={{ fontFamily: 'sans-serif', margin: 0 }}>
                        <h4 className='lead'>
                          <small className='text-muted'>
                            {item.manufacture}
                          </small>{' '}
                          {item.model}
                        </h4>
                        <p style={{ margin: 0 }}>{item.registration_number}</p>
                      </div>
                      {/* <div>
                    <Button variant="info" type="submit">
                      Email Receipt
                    </Button>
                  </div> */}
                    </div>
                  </Card.Body>
                </Accordion.Collapse>
              </Card>
            )
          })}
        </Accordion>
      </Col>
      <Col>
        <Trip tripData={bookings[selectedIndex]} />
      </Col>
    </div>
  )
}
