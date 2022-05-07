import { useEffect, useMemo } from 'react'
import DataTable from 'react-data-table-component'
import { useDispatch, useSelector } from 'react-redux'
import { keyBy } from 'lodash'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts'

import { loadAllTripsAction } from '../../app/actions'
import { getLocations } from '../home/HomeSlice'
import { getAllTrips } from './AdminSlice'

const tableColumns = [
  {
    name: 'Trip ID',
    selector: row => row.tripID,
    sortable: true
  },
  {
    name: 'Pickup Location',
    selector: row => row.pickup,
    sortable: true
  },
  {
    name: 'Destination',
    selector: row => row.dropoff,
    sortable: true
  },
  {
    name: 'Status',
    selector: row => row.status,
    sortable: true
  },
  {
    name: 'User ID',
    selector: row => row.userID,
    sortable: true
  },
  {
    name: 'Car ID',
    selector: row => row.carID,
    sortable: true
  }
]

export function AdminTrips () {
  const trips = useSelector(getAllTrips)
  const locations = useSelector(getLocations)
  const locationMap = useMemo(
    () => keyBy(locations, ({ waypoint }) => waypoint),
    [locations]
  )
  const tableTrips = useMemo(
    () => trips.map(trip => tripToTableTrip(trip, locationMap)),
    [trips, locationMap]
  )
  const locationData = useMemo(() => {
    const allLocations = Array.from(
      new Set(tableTrips.flatMap(trip => [trip.pickup, trip.dropoff]))
    )
    const results = Object.fromEntries(
      allLocations.map(name => [name, { name, pickup: 0, dropoff: 0 }])
    )
    tableTrips.forEach(trip => {
      results[trip.pickup].pickup += 1
      results[trip.dropoff].dropoff += 1
    })

    return Object.values(results)
  }, [tableTrips])

  const dispatch = useDispatch()
  useEffect(() => {
    dispatch(loadAllTripsAction())
  }, [dispatch])

  return (
    <div className='admin-main-page'>
      <h1>Trips</h1>
      <ResponsiveContainer className='trip-location-chart'>
        <BarChart
          data={locationData}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5
          }}
        >
          <CartesianGrid strokeDasharray='3 3' />
          <XAxis dataKey='name' />
          <YAxis />
          <Tooltip />
          <Legend />
          <Bar dataKey='pickup' fill='#8884d8' />
          <Bar dataKey='dropoff' fill='#82ca9d' />
        </BarChart>
      </ResponsiveContainer>
      <DataTable columns={tableColumns} data={tableTrips} pagination />
    </div>
  )
}

function tripToTableTrip (trip, locationMap) {
  return {
    tripID: trip.tripID,
    pickup: locationMap[trip.pickup_location].name,
    dropoff: locationMap[trip.dropoff_location].name,
    userID: trip.userID,
    status: getTripStatus(trip),
    carID: trip.carID
  }
}

function getTripStatus (trip) {
  if (trip.collision) {
    return 'Collision'
  } else if (trip.iscompleted) {
    return 'Completed'
  } else if (trip.isPickedUp) {
    return 'To Destination'
  } else if (trip.atPickUp) {
    return 'Picking up '
  } else {
    return 'To Pickup'
  }
}
