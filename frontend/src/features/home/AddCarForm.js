import { useCallback, useState } from 'react'
import { useDispatch } from 'react-redux'
import { Card, Form, Button } from 'react-bootstrap'

import { addUserCarAction } from '../../app/actions'

export const AddCarForm = ({ onSuccessCB, onCancel }) => {
  const [manufacturer, setManufacturer] = useState('')
  const [model, setModel] = useState('')
  const [regNo, setRegNo] = useState('')
  const dispatch = useDispatch()

  const addUserCar = useCallback(
    e => {
      if (
        manufacturer.length === 0 ||
        model.length === 0 ||
        regNo.length === 0
      ) {
        alert('Enter Valid Details')
        return false
      }
      e.preventDefault()
      dispatch(addUserCarAction({ manufacturer, model, regNo }))
      if (onSuccessCB) {
        onSuccessCB()
      }
    },
    [dispatch, manufacturer, regNo, model, onSuccessCB]
  )

  return (
    <Card.Body>
      <Card.Title
        className='lead'
        style={{ marginBottom: '2vh', marginLeft: '8vw' }}
      >
        Add Car
      </Card.Title>
      <Form style={{ width: '100%' }}>
        <Form.Group className='mb-3' controlId='formBasicEmail'>
          <Form.Label>Manufacturer</Form.Label>
          <Form.Control
            type='text'
            value={manufacturer}
            placeholder='Enter manufacturer'
            onChange={e => setManufacturer(e.target.value)}
          />
        </Form.Group>
        <Form.Group className='mb-3' controlId='formBasicEmail'>
          <Form.Label>Model</Form.Label>
          <Form.Control
            type='text'
            value={model}
            placeholder='Enter Model'
            onChange={e => setModel(e.target.value)}
          />
        </Form.Group>
        <Form.Group className='mb-3' controlId='formBasicEmail'>
          <Form.Label>Reg No</Form.Label>
          <Form.Control
            type='text'
            value={regNo}
            placeholder='Enter plate no'
            onChange={e => setRegNo(e.target.value)}
          />
        </Form.Group>
        <Form.Group className='mb-3'>
          <Button
            variant='info'
            // type="submit"
            style={{ alignSelf: 'center' }}
            onClick={e => addUserCar(e)}
          >
            Add Car
          </Button>
          {onCancel && <Button onClick={onCancel}>Cancel</Button>}
        </Form.Group>
      </Form>
    </Card.Body>
  )
}
