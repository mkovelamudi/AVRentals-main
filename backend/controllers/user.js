import UserModel from '../models/user.js'

// get all users list
export const getusers = (req, res) =>{
    console.log("All Users") 
   UserModel.getAllUsers((err, users)=>{
    if(err)
        res.send({status: false, message:err});
    console.log('Users', users);
    res.send({status: true, data:users})
   })
} 

export const getUserByID = (req, res) =>{
    UserModel.getUserByID(req.params.id, (err, user)=>{
     if(err)
         res.send(err);
     console.log('User', user);
     if(user.length == 0){
        res.send({status:false, message:'User Not Found'})
     }
     else
        res.send({status:true, data:user})
    })
 }

 export const userlogin = (req, res) =>{
    const userReqData = new UserModel(req.body)     
    //check null
    if(req.body.constructor === Object && Object.keys(req.body).length === 0){
        res.send(400).send({status:false, message: 'Please fill all the fields' })
    }
    else{  
        UserModel.userlogin(userReqData, (err, user) => {
            if(err)
                res.send(err)
            if(user == "Incorrect Username/Password." || user.length == 0){
                res.json({status:false, message:'Incorrect Username/Password. Login Failed'})
        }
            else
                res.json({status:true, message:'User data fetched for the username and password provided', data:user})
        })
    }
}

export const createUser = (req, res) =>{
     const userReqData = new UserModel(req.body)     
     //check null
     if(req.body.constructor === Object && Object.keys(req.body).length === 0){
         res.send(400).send({status:false, message: 'Please fill all the fields' })
     }
     else{ 
         
        UserModel.useremail(userReqData, (err, useremail) => {
            if(err)
               res.send({status: false, message:err})
            console.log("useremail is:", useremail)
            if(useremail.length == 0){
                UserModel.createUser(userReqData, (err, user) => {
                    if(err)
                        res.send({status: false, message:err})
                    console.log(user)
                    res.json({status:true, message:'User Registered Successfully', data:user})
                 })
            }

            else{
                res.send({status: false, message:"User Cannot be Registered. User with the same emailID already exists."})
            }
         })        
     }
 }

export const updateUser = (req, res) =>{
    const userReqData = new UserModel(req.body)     
    //check null
    if(req.body.constructor === Object && Object.keys(req.body).length === 0){
        res.send(400).send({status:false, message: 'Please fill all the fields' })
    }
    else{  
        UserModel.updateUser(req.params.id, userReqData, (err, user) => {
        if(err)
            res.send({status:false, message:'User Not Updated. Invalid Values Given.'})
        else if(user.affectedRows == 0) 
            res.send({status:false, message:'User Not Found'})
        else    
             res.json({status:true, message:'User Updated Successfully'})
        })
    }
} 

export const deleteUser = (req, res) =>{
    UserModel.deleteUser(req.params.id, (err, user)=>{
    if(err)
        res.send(err);
    else if(user.affectedRows == 0) 
        res.send({status:false, message:'User Not Found'})
    else
        res.json({status:true, message:'User Deleted Successfully'})
    })
 }