import Button from '@restart/ui/esm/Button'
import { useEffect, useState, useCallback } from 'react'
import {
  ButtonGroup,
  Form,
  FormControl,
  InputGroup,
  Modal
} from 'react-bootstrap'
import DataTable from 'react-data-table-component'
import { useDispatch, useSelector } from 'react-redux'

import { AddCarForm } from '../home/AddCarForm'
import {
  loadAllCarsAction,
  updateCarAction,
  deleteCarAction
} from '../../app/actions'
import { getAllCars } from './AdminSlice'

const columns = [
  {
    name: 'Car ID',
    selector: row => row.carID,
    sortable: true
  },
  {
    name: 'Manufacture',
    selector: row => row.manufacture,
    sortable: true
  },
  {
    name: 'Model',
    selector: row => row.model,
    sortable: true
  },
  {
    name: 'Registration #',
    selector: row => row.registration_number
  },
  {
    name: 'Owner ID',
    selector: row => row.userID,
    sortable: true
  }
]

export function AdminCars () {
  const cars = useSelector(getAllCars)
  const [showAddCarModal, setShowAddCarModal] = useState(false)
  const dispatch = useDispatch()

  useEffect(() => {
    dispatch(loadAllCarsAction())
  }, [dispatch, showAddCarModal])

  return cars && cars.length > 0 ? (
    <div className='admin-main-page'>
      <h1>Cars</h1>
      <DataTable
        columns={columns}
        data={cars}
        expandableRows={true}
        expandableRowsComponent={AdminCarDetails}
        actions={
          <button
            className='add-user-button'
            onClick={() => setShowAddCarModal(true)}
          >
            Add Car
          </button>
        }        
        pagination
      />
      <Modal show={showAddCarModal}>
        <AddCarForm 
          onSuccessCB={() => setShowAddCarModal(false)}
          onCancel={() => setShowAddCarModal(false)}
        />
      </Modal>
    </div>
  ) : (
    <h1>Cars</h1>
  )
}

const AdminCarDetails = ({ data }) => {
  const { carID } = data
  const [manufacture, setManufacture] = useState(data.manufacture)
  const [model, setModel] = useState(data.model)
  const [registration_number, setRegistrationNumber] = useState(
    data.registration_number
  )
  const [userID, setUserID] = useState(data.userID)
  const dispatch = useDispatch()

  const onEdit = useCallback(() => {
    if (
      manufacture.length === 0 ||
      model.length === 0 ||
      registration_number.length === 0 ||
      userID.length === 0
    ) {
      window.alert('All fields are required')
    } else if (
      manufacture === data.manufacture &&
      model === data.model &&
      registration_number === data.registration_number &&
      userID === data.userID
    ) {
      window.alert('No change')
    } else {
      dispatch(
        updateCarAction({
          ...data,
          manufacture,
          model,
          registration_number,
          userID
        })
      )
    }
  }, [manufacture, model, registration_number, userID, data, dispatch])

  const onDelete = useCallback(() => {
    const confirmed = window.confirm(
      `Do you really want to delete car [${carID}]? `
    )
    if (confirmed) {
      dispatch(
        deleteCarAction({
          carID
        })
      )
    }
  }, [carID, dispatch])

  return (
    <Form className='edit-car-form'>
      <InputGroup>
        <InputGroup.Text>ID: </InputGroup.Text>
        <FormControl disabled={true} value={userID} />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Manufacture: </InputGroup.Text>
        <FormControl
          value={manufacture}
          onChange={e => setManufacture(e.target.value)}
        />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Model: </InputGroup.Text>
        <FormControl value={model} onChange={e => setModel(e.target.value)} />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Registration #: </InputGroup.Text>
        <FormControl
          value={registration_number}
          onChange={e => setRegistrationNumber(e.target.value)}
        />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Owner ID: </InputGroup.Text>
        <FormControl value={userID} onChange={e => setUserID(e.target.value)} />
      </InputGroup>
      <ButtonGroup className='edit-user-form-buttons'>
        <Button onClick={onEdit}>Edit</Button>
        <Button onClick={onDelete}>Delete</Button>
      </ButtonGroup>
    </Form>
  )
}
