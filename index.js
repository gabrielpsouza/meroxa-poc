exports.App = class App {
  anonymize(records) {
    const filteredRecords = records.filter((record) => {
      const product = record.get("product");
      const subProduct = record.get("sub_product");
      return product === "Somente Financeiro" && subProduct === "Saldo em Conta";
    });

    // Use records `unwrap` transform on CDC formatted records
    // Has no effect on other formats
    records.unwrap();

    return records;
  }

  async run(turbine) {
    let source = await turbine.resources("mysqldb-test-nlb");

    let records = await source.records("investors_portfolio_daily_data_latest");

    let anonymized = await turbine.process(records, this.anonymize);

    let destination = await turbine.resources("s3-meroxa");

    await destination.write(anonymized, "meroxa-poc",  {
      "data.from.investors_portfolio_daily_data_latest": "{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=yyyy}}{{timestamp:unit=MM}}{{timestamp:unit=dd}}{{timestamp:unit=HH}}.gz"
    });
  }
};
