exports.App = class App {
  anonymize(records) {
    const filteredRecords = records.filter((record) => {
      const product = record.get("product");
      const subProduct = record.get("sub_product");
      return product === "Somente Financeiro" && subProduct === "Saldo em Conta";
    });

    filteredRecords.forEach((record) => {      
      record.set(
        "customer_email",
        iAmHelping(stringHash(record.get("customer_email")))
      );
    });
    records.unwrap();

    return filteredRecords; 
  }

  async run(turbine) {
    let source = await turbine.resources("mysqldb");
    console.log('source', source);
    let records = await source.records("investors_data_latest");
    let anonymized = await turbine.process(records, this.anonymize);
    console.log('anonymized', anonymized);

    let destination = await turbine.resources("s3-meroxa");
    await destination.write(anonymized, "s3-meroxa");
  }
};