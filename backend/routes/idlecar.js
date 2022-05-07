import express from 'express'
import {getIdlecar } from '../controllers/idlecar.js';

const router = express.Router() ;

//get TripByID
router.get('/findcar', getIdlecar);

export default router ;