import React, { Fragment } from "react";
import { Text, View, StyleSheet } from "@react-pdf/renderer";

const borderColor = "#90e5fc";
const styles = StyleSheet.create({
  row: {
    flexDirection: "row",
    borderBottomColor: "#bff0fd",
    borderBottomWidth: 1,
    alignItems: "center",
    height: 24,
    textAlign: "center",
    fontStyle: "bold",
    flexGrow: 1,
  },
  description: {
    width: "60%",
    textAlign: "left",
    borderRightColor: borderColor,
    borderRightWidth: 1,
  },
  qty: {
    width: "10%",
    textAlign: "right",
    borderRightColor: borderColor,
    borderRightWidth: 1,
    paddingRight: 2,
  },
  rate: {
    width: "15%",
    textAlign: "right",
    borderRightColor: borderColor,
    borderRightWidth: 1,
    paddingRight: 2,
  },
  amount: {
    textAlign: "right",
    width: "15%",
    paddingRight: 2,
  },
});

const InvoiceTableRow = ({ tripData }) => {
  return (
    <Fragment>
      <View style={styles.row}>
        <Text style={styles.description}>Base Charge</Text>
        <Text style={styles.qty}>{`${tripData.miles} mi`}</Text>
        <Text style={styles.rate}>$ 4.00/mi</Text>
        <Text style={styles.amount}>
          $ {`${(tripData.miles * 4).toFixed(2)}`}
        </Text>
      </View>
      <View style={styles.row}>
        <Text style={styles.description}>Booking Charge</Text>
        <Text style={styles.qty}>{"-  "}</Text>
        <Text style={styles.rate}>{"-  "}</Text>
        <Text style={styles.amount}>
          $ {`${(tripData.miles * 1).toFixed(2)}`}
        </Text>
      </View>
      <View style={styles.row}>
        <Text style={styles.description}>Taxes</Text>
        <Text style={styles.qty}>{"-  "}</Text>
        <Text style={styles.rate}>10%</Text>
        <Text style={styles.amount}>$ {`${tripData.tax.toFixed(2)}`}</Text>
      </View>
    </Fragment>
  );
};

export default InvoiceTableRow;
