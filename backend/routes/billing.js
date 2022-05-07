import express from 'express'
import { getbilling, getBillingByID, createBilling, updateBilling, deleteBilling } from '../controllers/billing.js';

const router = express.Router() ;

//get all billing
router.get('/all', getbilling) ;

//get BillingByID
router.get('/:id', getBillingByID);

//Create New Bill
router.post('/add', createBilling) ;

//Update Bill
router.put('/edit/:id', updateBilling);

//Delete Bill
router.delete('/:id',deleteBilling);

export default router ;