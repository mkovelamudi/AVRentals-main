import { useEffect } from 'react'
import DataTable from 'react-data-table-component'
import { useDispatch, useSelector } from 'react-redux'
import { countBy } from 'lodash'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

import { loadAllBillingsAction } from '../../app/actions'
import { getAllBilllings } from './AdminSlice'

const tableColumns = [
  {
    name: 'Billing ID',
    selector: row => row.tripID,
    sortable: true
  },
  {
    name: 'Miles',
    selector: row => row.miles,
    sortable: true
  },
  {
    name: 'Fare',
    selector: row => row.cost,
    sortable: true
  },
  {
    name: 'Tax & Fee',
    selector: row => row.tax,
    sortable: true
  },
  {
    name: 'Total Cost',
    selector: row => row.total_cost,
    sortable: true
  }
]

export function AdminBillings () {
  const billings = useSelector(getAllBilllings)
  const distanceChartData = Object.entries(countBy(
    billings,
    bill => Math.floor(bill.miles * 10) / 10
  )).map(([distance, count]) => ({distance: Number(distance), count}))

  const dispatch = useDispatch()
  useEffect(() => {
    dispatch(loadAllBillingsAction())
  }, [dispatch])

  return (
    <div className='admin-main-page'>
      <h1>Billings</h1>
      <ResponsiveContainer className='trip-location-chart'>
        <BarChart
          data={distanceChartData}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5
          }}
        >
          <CartesianGrid strokeDasharray='3 3' />
          <XAxis dataKey='distance' />
          <YAxis />
          <Tooltip />
          <Legend />
          <Bar dataKey='count' fill='#8884d8' />
        </BarChart>
      </ResponsiveContainer>
      <DataTable columns={tableColumns} data={billings} pagination />
    </div>
  )
}
