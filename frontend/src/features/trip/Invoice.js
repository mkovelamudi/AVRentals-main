import React, { Fragment, useEffect } from "react";
import {
  Page,
  Text,
  View,
  Document,
  StyleSheet,
  Image,
  PDFDownloadLink,
} from "@react-pdf/renderer";
import moment from "moment";
import logoImage from "../../assets/logo.jpg";
import InvoiceTableHeader from "./InvoiceTableHeader";
import InvoiceTableRow from "./InvoiceTableRow";
import faker from "faker";

const styles = StyleSheet.create({
  page: {
    fontFamily: "Helvetica",
    fontSize: 11,
    paddingTop: 10,
    paddingLeft: 60,
    paddingRight: 60,
    lineHeight: 1.5,
    flexDirection: "column",
  },
  logo: {
    width: 74,
    height: 66,
    marginLeft: "auto",
    marginRight: "auto",
  },
  titleContainer: {
    flexDirection: "row",
    alignContent: "center",
    justifyContent: "center",
    marginTop: 12,
  },
  reportTitle: {
    color: "#61dafb",
    letterSpacing: 4,
    fontSize: 25,
    textAlign: "center",
    textTransform: "uppercase",
  },
  invoiceNoContainer: {
    flexDirection: "row",
    marginTop: 10,
    justifyContent: "flex-end",
  },
  invoiceDateContainer: {
    flexDirection: "row",
    justifyContent: "flex-end",
  },
  invoiceDate: {
    fontSize: 12,
    fontStyle: "bold",
  },
  label: {
    width: "15vw",
  },
  headerContainer: {
    marginTop: 0,
  },
  billTo: {
    marginTop: 20,
    paddingBottom: 3,
    fontFamily: "Helvetica-Oblique",
  },
  tableContainer: {
    flexDirection: "row",
    flexWrap: "wrap",
    marginTop: 24,
    borderWidth: 1,
    borderColor: "#bff0fd",
  },
  row: {
    flexDirection: "row",
    borderBottomColor: "#bff0fd",
    borderBottomWidth: 1,
    alignItems: "center",
    height: 24,
    fontSize: 12,
    fontStyle: "bold",
  },
  description: {
    width: "85%",
    textAlign: "right",
    borderRightColor: "#90e5fc",
    borderRightWidth: 1,
    paddingRight: 8,
  },
  total: {
    width: "15%",
    textAlign: "right",
    paddingRight: 2,
  },
});

function invoiceDocument({ tripData, user }) {
  return (
    <Document>
      <Page size="A4" style={styles.page}>
        <Image style={styles.logo} src={logoImage} />
        <View style={styles.titleContainer}>
          <Text style={styles.reportTitle}>Invoice</Text>
        </View>

        <Fragment>
          <View style={styles.invoiceNoContainer}>
            <Text
              style={styles.invoiceDate}
            >{`Invoice No: ${faker.random.number({
              min: 214566,
              max: 999999,
            })}-${faker.random.number(99)}`}</Text>
          </View>
          <View style={styles.invoiceDateContainer}>
            <Text>{moment(tripData.start_time).format("ddd, Do MMM 'YY")}</Text>
          </View>
          <View style={styles.invoiceDateContainer}>
            <Text>{moment(tripData.start_time).format("LT")}</Text>
          </View>
        </Fragment>

        <View style={styles.headerContainer}>
          <Text style={styles.billTo}>Bill To:</Text>
          <Text>{`${user.firstname} ${user.lastname}`}</Text>
          <Text>{`${user.address}`}</Text>
          <Text>{`${user.phone}`}</Text>
        </View>

        <View style={styles.tableContainer}>
          <InvoiceTableHeader />
          <InvoiceTableRow tripData={tripData} />

          <View style={styles.row}>
            <Text style={styles.description}>Total Amount</Text>
            <Text style={styles.total}>$ {tripData.total_cost.toFixed(2)}</Text>
          </View>
        </View>
        <View
          style={{
            flexDirection: "row",
            marginTop: 12,
            justifyContent: "center",
          }}
        >
          <Text
            style={{
              fontSize: 12,
              textAlign: "center",
              textTransform: "uppercase",
            }}
          >
            Thank you for your business
          </Text>
        </View>
      </Page>
    </Document>
  );
}
export function PdfInvoice({ tripData, user }) {
  return (
    <PDFDownloadLink
      document={invoiceDocument({ tripData, user })}
      fileName="tripReceipt.pdf"
    >
      Download Receipt
    </PDFDownloadLink>
  );
}
