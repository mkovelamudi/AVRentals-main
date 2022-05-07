const validateEmail = (email) => {
  return new RegExp(
    /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
  ).test(email);
};

const validatePassword = (password) => {
  return password !== null && password !== undefined && password.length > 4;
};

module.exports = {
  validateEmail,
  validatePassword,
};
