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

import {
  loadAllUsersAction,
  updateUserAction,
  deleteUserAction
} from '../../app/actions'
import { RegisterFormCard } from '../auth/RegisterFormCard'
import { getAllUsers } from './AdminSlice'

const columns = [
  {
    name: 'First Name',
    selector: row => row.firstname,
    sortable: true
  },
  {
    name: 'Last Name',
    selector: row => row.lastname,
    sortable: true
  },
  {
    name: 'Email',
    selector: row => row.user_email,
    sortable: true
  },
  {
    name: 'Role',
    selector: row => row.role,
    sortable: true
  },
  {
    name: 'Phone',
    selector: row => row.phone
  },
  {
    name: 'Address',
    selector: row => row.address
  }
]

export function AdminUsers () {
  const users = useSelector(getAllUsers)
  const [showAddUserModal, setShowAddUserModal] = useState(false)
  const dispatch = useDispatch()

  useEffect(() => {
    dispatch(loadAllUsersAction())
  }, [dispatch])

  return users && users.length > 0 ? (
    <div className='admin-main-page'>
      <h1>Users</h1>
      <DataTable
        columns={columns}
        data={users}
        expandableRows={true}
        expandableRowsComponent={AdminUserDetails}
        actions={
          <button
            className='add-user-button'
            onClick={() => setShowAddUserModal(true)}
          >
            Add User
          </button>
        }
        pagination
      />
      <Modal show={showAddUserModal}>
        <RegisterFormCard
          submitButtonText='Add User'
          setShowLogin={() => setShowAddUserModal(false)}
          showLoginText='Cancel'
          onSuccessCB={() => setShowAddUserModal(false)}
        />
      </Modal>
    </div>
  ) : (
    <h1>Admin User Page</h1>
  )
}

const AdminUserDetails = ({ data }) => {
  const { userID } = data
  const [user_email, setEmail] = useState(data.user_email)
  const [firstname, setFirstname] = useState(data.firstname)
  const [lastname, setLastname] = useState(data.lastname)
  const [phone, setPhone] = useState(data.phone)
  const [address, setAddress] = useState(data.address)
  const dispatch = useDispatch()

  const onEdit = useCallback(() => {
    if (
      user_email.length === 0 ||
      firstname.length === 0 ||
      lastname.length === 0 ||
      phone.length === 0 ||
      address.length === 0
    ) {
      window.alert('All fields are required')
    } else if (
      user_email === data.user_email &&
      firstname === data.firstname &&
      lastname === data.lastname &&
      phone === data.phone &&
      address === data.address
    ) {
      window.alert('No change')
    } else {
      dispatch(
        updateUserAction({
          ...data,
          user_email,
          firstname,
          lastname,
          phone,
          address
        })
      )
    }
  }, [user_email, firstname, lastname, phone, address, dispatch, data])

  const onDelete = useCallback(() => {
    const confirmed = window.confirm(
      `Do you really want to delete user [${firstname + lastname}]? `
    )
    if (confirmed) {
      dispatch(
        deleteUserAction({
          userID
        })
      )
    }
  }, [userID, dispatch, firstname, lastname])

  return (
    <Form className='edit-user-form'>
      <InputGroup>
        <InputGroup.Text>ID: </InputGroup.Text>
        <FormControl disabled={true} value={userID} />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Email: </InputGroup.Text>
        <FormControl
          value={user_email}
          onChange={e => setEmail(e.target.value)}
        />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>First: </InputGroup.Text>
        <FormControl
          value={firstname}
          onChange={e => setFirstname(e.target.value)}
        />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Last: </InputGroup.Text>
        <FormControl
          value={lastname}
          onChange={e => setLastname(e.target.value)}
        />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Phone: </InputGroup.Text>
        <FormControl value={phone} onChange={e => setPhone(e.target.value)} />
      </InputGroup>
      <InputGroup>
        <InputGroup.Text>Address: </InputGroup.Text>
        <FormControl
          value={address}
          onChange={e => setAddress(e.target.value)}
        />
      </InputGroup>
      <ButtonGroup className='edit-user-form-buttons'>
        <Button onClick={onEdit}>Edit</Button>
        <Button onClick={onDelete}>Delete</Button>
      </ButtonGroup>
    </Form>
  )
}
