// Import any of your relevant dependencies
const stringHash = require("string-hash");

// Sample helper
function iAmHelping(str) {
  return `~~~${str}~~~`;
}

exports.App = class App {
  // Create a custom named function on the App to be applied to your records
  anonymize(records) {
    records.forEach((record) => {
      // Use record `get` and `set` to read and write to your data
      record.set(
        "customer_email",
        iAmHelping(stringHash(record.get("customer_email")))
      );
    });

    // Use records `unwrap` transform on CDC formatted records
    // Has no effect on other formats
    records.unwrap();

    return records;
  }

  async run(turbine) {
    let source = await turbine.resources("mysqldb");

    let records = await source.records("investors_portfolio_daily_data_latest");

    let anonymized = await turbine.process(records, this.anonymize);

    let destination = await turbine.resources("s3-meroxa");

    await destination.write(anonymized, "meroxa-poc",  {
      "data.from.investors_portfolio_daily_data_latest": "{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=yyyy}}{{timestamp:unit=MM}}{{timestamp:unit=dd}}{{timestamp:unit=HH}}.gz"
    });
    console.log('chegou no final');
  }
};
