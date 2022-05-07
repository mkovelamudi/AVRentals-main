import express from 'express'
import { getusers, userlogin, getUserByID, createUser, updateUser, deleteUser } from '../controllers/user.js';
//import user from '../models/user.js' ;
const router = express.Router() ;

//get all users
router.get('/', getusers) ;

//user login
router.post('/login', userlogin) ;

//get UserByID
router.get('/:id', getUserByID);

//Create New User
router.post('/register', createUser) ;

//Update User
router.put('/editprofile/:id', updateUser);

//Delete user
router.delete('/:id',deleteUser);


export default router ;