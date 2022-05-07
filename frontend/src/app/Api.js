import axios from "axios";
// const API_BASE_URL = "http://18.188.8.34:4000";
const API_BASE_URL = "http://localhost:3001";

export async function post({ endpoint, body }) {
  try {
    const response = await axios.post(`${API_BASE_URL}/${endpoint}`, body);
    console.log({ endpoint, body, response });
    if (!response.data.status) {
      console.log(response.data.message);
    }
    return response.data;
  } catch (error) {
    console.log(error);
    alert(`endpoint: ${endpoint} + error: ${error.toString()}`);
    return {
      status: false,
      meesage: error.toString(),
    };
  }
}

export async function put({ endpoint, body }) {
  try {
    const response = await axios.put(`${API_BASE_URL}/${endpoint}`, body);
    console.log({ endpoint, body, response });
    if (!response.data.status) {
      console.log(response.data.message);
    }
    return response.data;
  } catch (error) {
    console.log(error);
    alert(`endpoint: ${endpoint} + error: ${error.toString()}`);
    return {
      status: false,
      meesage: error.toString(),
    };
  }
}

export async function get({ endpoint, params }) {
  try {
    const response = await axios.get(`${API_BASE_URL}/${endpoint}`, params);
    if (!response.data.status) {
      console.log(response.data.message);
    }
    return response.data;
  } catch (error) {
    console.log(error);
    alert(`endpoint: ${endpoint} + error: ${error.toString()}`);
    return {
      status: 500,
      meesage: error.toString(),
    };
  }
}

export async function deleteApi({ endpoint }) {
  try {
    const response = await axios.delete(`${API_BASE_URL}/${endpoint}`);
    if (!response.data.status) {
      console.log(response.data.message);
    }
    return response.data;
  } catch (error) {
    console.log(error);
    alert(error.toString());
    return {
      status: 500,
      meesage: error.toString(),
    };
  }
}
