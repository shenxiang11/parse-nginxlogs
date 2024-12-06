use arrow::array;
use arrow::array::{StringArray, UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use regex::Regex;
use std::fs;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct NginxLog {
    addr: String,
    datetime: String,
    method: String,
    url: String,
    protocol: String,
    status: u16,
    body_bytes: u64,
    referer: String,
    user_agent: String,
}

fn write_parquet(data: Vec<NginxLog>, output_path: &str) -> anyhow::Result<()> {
    let schema = Schema::new(vec![
        Field::new("addr", DataType::Utf8, false),
        Field::new("datetime", DataType::Utf8, false),
        Field::new("method", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("status", DataType::UInt16, false),
        Field::new("body_bytes", DataType::UInt64, false),
        Field::new("referer", DataType::Utf8, false),
        Field::new("user_agent", DataType::Utf8, false),
    ]);

    let addrs: Vec<_> = data.iter().map(|log| log.addr.clone()).collect();
    let datetimes: Vec<_> = data.iter().map(|log| log.datetime.clone()).collect();
    let methods: Vec<_> = data.iter().map(|log| log.method.clone()).collect();
    let urls: Vec<_> = data.iter().map(|log| log.url.clone()).collect();
    let protocols: Vec<_> = data.iter().map(|log| log.protocol.clone()).collect();
    let statuses: Vec<_> = data.iter().map(|log| log.status).collect();
    let body_bytes: Vec<_> = data.iter().map(|log| log.body_bytes).collect();
    let referers: Vec<_> = data.iter().map(|log| log.referer.clone()).collect();
    let user_agents: Vec<_> = data.iter().map(|log| log.user_agent.clone()).collect();

    let addr_array = StringArray::from(addrs);
    let datetime_array = StringArray::from(datetimes);
    let method_array = StringArray::from(methods);
    let url_array = StringArray::from(urls);
    let protocol_array = StringArray::from(protocols);
    let status_array = UInt16Array::from(statuses);
    let body_bytes_array = UInt64Array::from(body_bytes);
    let referer_array = StringArray::from(referers);
    let user_agent_array = StringArray::from(user_agents);

    let batch = Arc::new(arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(addr_array),
            Arc::new(datetime_array),
            Arc::new(method_array),
            Arc::new(url_array),
            Arc::new(protocol_array),
            Arc::new(status_array),
            Arc::new(body_bytes_array),
            Arc::new(referer_array),
            Arc::new(user_agent_array),
        ],
    )?);

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let logs = fs::read_to_string("./fixtures/logs.txt")?;
    let mut results: Vec<NginxLog> = Vec::new();
    let re = Regex::new(
        r#"(?m)^(?<ip>\S+)\s+\S+\s+\S+\s+\[(?<date>[^\]]+)\]\s+"(?<method>\S+)\s+(?<url>\S+)\s+(?<proto>[^"]+)"\s+(?<status>\d+)\s+(?<bytes>\d+)\s+"(?<referer>[^"]+)"\s+"(?<ua>[^"]+)"$"#,
    )?;
    for cap in re.captures_iter(&logs) {
        let log = NginxLog {
            addr: cap["ip"].to_string(),
            datetime: cap["date"].to_string(),
            method: cap["method"].to_string(),
            url: cap["url"].to_string(),
            protocol: cap["proto"].to_string(),
            status: cap["status"].parse()?,
            body_bytes: cap["bytes"].parse()?,
            referer: cap["referer"].to_string(),
            user_agent: cap["ua"].to_string(),
        };
        results.push(log);
    }

    write_parquet(results, "./fixtures/nginx_log.parquet")?;
    Ok(())
}
