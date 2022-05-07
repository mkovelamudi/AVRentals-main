import dbConn from "../config/db.config.js";
import bcrypt from "bcrypt";
var UserModel = function (user) {
  (this.userID = user.userID),
    (this.password = user.password),
    (this.user_email = user.user_email),
    (this.firstname = user.firstname);
  this.lastname = user.lastname;
  (this.address = user.address),
    (this.phone = user.phone),
    (this.role = user.role);
};

//get all users
UserModel.getAllUsers = (result) => {
  dbConn.query("SELECT * FROM users", (err, res) => {
    if (err) {
      console.log("Error while fetching users", err);
      result(null, err);
    } else {
      console.log("Users fetched successfully");
      result(null, res);
    }
  });
};

//get userByID from DB
UserModel.getUserByID = (id, result) => {
  dbConn.query(`SELECT * FROM users WHERE userID=?`, id, (err, res) => {
    if (err) {
      console.log("Error while fetching users", err);
      result(err, null);
    } else {
      result(null, res);
    }
  });
};

//get User EMail

UserModel.useremail = async (userReqData, result) => {
  //userReqData.password = await bcrypt.hash(userReqData.password, 10);
  // dbConn.query(`SELECT * FROM users WHERE user_email= and password= ?`,[userReqData.user_email, userReqData.password] ,  (err, res)=>{
  dbConn.query(
    `SELECT * FROM users WHERE user_email= ?`,
    [userReqData.user_email],
    async (err, res) => {
      if (err) {
        console.log("Error while fetching user", err);
        result(err, null);
      } else {
        result(null, res);
      }
    }
  );
};

//get userByID from DB
UserModel.userlogin = async (userReqData, result) => {
  //userReqData.password = await bcrypt.hash(userReqData.password, 10);
  // dbConn.query(`SELECT * FROM users WHERE user_email= and password= ?`,[userReqData.user_email, userReqData.password] ,  (err, res)=>{
  dbConn.query(
    `SELECT * FROM users WHERE user_email= ?`,
    [userReqData.user_email],
    async (err, res) => {
      let isEqual = false;
      if (res.length != 0)
        isEqual = await bcrypt.compare(userReqData.password, res[0].password);
      console.log("isEqual:", isEqual);
      if (err) {
        console.log("Error while fetching user", err);
        result(err, null);
      } else if (res.length == 0) {
        console.log("User doesnot exist");
        result(null, res);
      } else if (isEqual) {
        console.log("User details fetched successfully.");
        result(null, res);
      } else {
        result(null, "Incorrect Username/Password.");
      }
    }
  );
};

/* POST JSON Details for Insomnia to create a new user
{
	"password":"pass3",
	"user_email":"some3@some.com",
	"firstname":"ccc",
	"lastname":"fff",
	"address":"123, sad, dsfs",
	"phone":1234,
	"role":"owner"
}*/

UserModel.createUser = async (userReqData, result) => {
  userReqData.password = await bcrypt.hash(userReqData.password, 10);
  dbConn.query("INSERT INTO users SET ?", userReqData, (err, res) => {
    if (err) {
      console.log("Error while inserting userData", err);
      result(err, null);
    } else {
      result(null, res);
    }
  });
};

//Update User
UserModel.updateUser = async (id, userReqData, result) => {
  // userReqData.password = await bcrypt.hash(userReqData.password, 10);
  // console.log("Password Ecry: ",userReqData.password)
  dbConn.query(
    "UPDATE users SET password=?, user_email = ?, firstname = ?, lastname = ?, address = ?, phone = ?, role = ? WHERE userID = ?",
    [
      userReqData.password,
      userReqData.user_email,
      userReqData.firstname,
      userReqData.lastname,
      userReqData.address,
      userReqData.phone,
      userReqData.role,
      id,
    ],
    (err, res) => {
      if (err) {
        console.log("Error while updating userData", err);
        result(err, null);
      } else {
        result(null, res);
      }
    }
  );
};

//Delete User
UserModel.deleteUser = (id, result) => {
  dbConn.query("DELETE FROM users WHERE userID = ?", id, (err, res) => {
    if (err) {
      console.log("Error while deleting userData", err);
      result(err, null);
    } else {
      result(null, res);
    }
  });
};

//module.exports = User ;
export default UserModel;
