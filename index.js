exports.App = class App {
  anonymize(records) {   
    console.log('RECORDS REGISTER', records)
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
