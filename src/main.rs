use std::{fs::{self, File}, io::{Read, Write}, path::Path, sync::Arc, thread, time::{Duration, SystemTime, UNIX_EPOCH}};
use std::fmt::Write as FmtWrite;

use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use reqwest::{header::{self, HeaderMap, HeaderValue}, Client, Response};
use serde_json::Value;
use tokio::{fs::OpenOptions, io::AsyncReadExt, sync::Semaphore};
use tokio::io::AsyncWriteExt;
use futures::stream::{FuturesUnordered, StreamExt};
use clap::Parser;

const PACKAGES_LIST_URL: &str = "https://packagist.org/packages/list.json";
const CHANGES_URL: &str = "https://packagist.org/metadata/changes.json?since=";
const PACKAGE_METADATA_URL: &str = "https://packagist.org/p2/";
const PUBLIC_PATH: &str = "./public/";

/// Quickly build a composer mirror.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Simultaneously update the concurrency of extension packages
    #[arg(short, long, default_value_t = 10)]
    concurrency: usize,

    /// Interval for executing synchronization (unit: seconds)
    #[arg(short, long, default_value_t = 60)]
    interval: u64,

    /// Proxy address
    #[arg(short, long)]
    proxy: Option<String>
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {

    let args = Args::parse();

    let client = Client::builder();

    let client = match args.proxy {
        Some(proxy) => client.proxy(reqwest::Proxy::all(proxy)?).build()?,
        None => client.build()?
    };
       

    loop {
        let last_modified_time = read_last_modified_time();
        if let Some(last_modified_time) = last_modified_time {
            // Incremental update
            if let Err(e) = incremental_update(&client, last_modified_time).await {
                eprintln!("Incremental update failed: {:?}", e);
            }
        } else {
            // Full update
            if let Err(e) = full_update(&client).await {
                eprintln!("Full update failed: {:?}", e);
            }
        }

        thread::sleep(Duration::from_secs(args.interval));
    }
}


fn read_last_modified_time() -> Option<u64> {
    if Path::new("last-modified").exists() {
        let mut file = File::open("last-modified").ok()?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).ok()?;
        contents.trim().parse().ok()
    } else {
        None
    }
}

fn save_last_modified_time(timestamp: &str) {
    let mut file = File::create("last-modified").expect("Unable to create last-modified file");
    file.write_all(timestamp.as_bytes())
        .expect("Unable to write timestamp to file");
}

async fn load_url(url: &str, client: &Client) -> Result<BytesMut, anyhow::Error> {

    let total_size = {
        let resp = client.head(url).send().await?;
        if resp.status().is_success() {
            resp.headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|ct_len| ct_len.to_str().ok())
                .and_then(|ct_len| ct_len.parse().ok())
                .unwrap_or(0)
        } else {
            return Err(anyhow::anyhow!(
                "Couldn't download URL: {}. Error: {:?}",
                url,
                resp.status(),
            ));
        }
    };

    let request = client.get(url)
        .header(header::ACCEPT, "*/*")
        .header(header::ACCEPT_ENCODING, "gzip, deflate, br")
        .header(header::CONNECTION, "keep-alive");

    let pb = create_url_load_bar(total_size as u64);

    let mut source = request.send().await?;
    let mut dest = BytesMut::with_capacity(0);
    while let Some(chunk) = source.chunk().await? {
        dest.extend_from_slice(&chunk);
        pb.inc(chunk.len() as u64);
    }

    Ok(dest)
}

async fn fetch_with_retries(client: &Client, url: &str, retries: i32, headers: HeaderMap) -> Result<Response, anyhow::Error> {
    for attempt in 0..=retries {
        
        let response_result = client
            .get(url)
            .headers(headers.clone())
            .send()
            .await;

        match response_result {
            Ok(response) => return Ok(response),
            Err(e) if attempt < retries => {
                println!("Failed to download. Error: {}. Retrying...", e);
                thread::sleep(Duration::from_secs(2)); 
            },
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
    }

    Err(anyhow::anyhow!("Max retries exceeded"))
}

async fn full_update(client: &Client) -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let byt = load_url(PACKAGES_LIST_URL, client).await?;

    let mut decoder = GzipDecoder::new(&byt[..]);
    let mut content = String::new();
    decoder.read_to_string(&mut content).await?;

    let response:Value = serde_json::from_str(&content).unwrap();

    let start = SystemTime::now();
    let duration = start.duration_since(UNIX_EPOCH)?;

    let package_names: Vec<String> = match response["packageNames"].as_array() {
        Some(packages) => packages
            .iter()
            .filter_map(|pkg| pkg.as_str().map(|s| s.to_string()))
            .collect(),
        None => vec![],
    };


    let pb = create_packages_download_bar(package_names.len() as u64);
    let sem = Arc::new(Semaphore::new(args.concurrency));

    let mut tasks = FuturesUnordered::new();

    for package_name  in package_names {
        let sem = Arc::clone(&sem);
        let client = client.clone();
        let pb = pb.clone();

        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await?;
            fetch_package_metadata(&client, &package_name).await?;
            fetch_package_metadata(&client, &format!("{}~dev", &package_name).as_str()).await?;
            pb.inc(1);

            Result::<(), anyhow::Error>::Ok(())
        }));
    }

    while let Some(result) = tasks.next().await {
        result??;
    }
    

    save_last_modified_time(format!("{}0", duration.as_millis().to_string()).as_str());
    Ok(())
}

async fn incremental_update(client: &Client, last_modified_time: u64) -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let url = format!("{}{}", CHANGES_URL, last_modified_time);
    let response: Value = client.get(&url).send().await.unwrap().json().await.unwrap();

    match response["actions"][0]["type"].as_str() {
        Some("resync") => {
            full_update(client).await?;
        },
        None => {
            return Ok(());
        },
        _ => {

            let actions = response["actions"]
                .as_array()
                .cloned() 
                .unwrap_or_else(Vec::new);

            let pb = create_packages_download_bar(actions.len() as u64);

            let sem = Arc::new(Semaphore::new(args.concurrency));

            let mut tasks = FuturesUnordered::new();
            

            for action in actions {
                let sem = Arc::clone(&sem);
                let client = client.clone();
                let pb = pb.clone();

                tasks.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await?;

                    match action["type"].as_str() {
                        Some("delete") => {
                            delete_package_metadata(action["package"].as_str().unwrap());
                        },
                        _ => {
                            fetch_package_metadata(&client, action["package"].as_str().unwrap()).await?;
                        }
                    }
    
                    pb.inc(1);
                    Result::<(), anyhow::Error>::Ok(())
                }));

                
            }

            while let Some(result) = tasks.next().await {
                result??;
            }
            
        }
        
    }

    if response["timestamp"].is_u64() {
        save_last_modified_time(response["timestamp"].to_string().as_str());
    }
    Ok(())
}

fn delete_package_metadata(package_name: &str) {
    let paths = vec![
        format!("{}p2/{}.json", PUBLIC_PATH, package_name),
        format!("{}p2/{}~dev.json", PUBLIC_PATH, package_name),
    ];

    for path in paths {
        fs::remove_file(&path).ok();
    }
}

async fn fetch_package_metadata(client: &Client, package_name: &str) -> Result<(), anyhow::Error> {
    let path = format!("{}p2/{}.json", PUBLIC_PATH, package_name);
    let mut headers = HeaderMap::new();
    headers.insert("Accept-Encoding", HeaderValue::from_static("gzip"));
    
    if Path::new(&path).exists() {
        let metadata = fs::metadata(&path)?;

        let system_time = metadata.modified()?;
        let datetime: DateTime<Utc> = DateTime::from(system_time);
        // 格式化日期时间为 RFC2822 格式
        let formatted_date = datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

        headers.insert("If-Modified-Since", HeaderValue::from_str(&formatted_date)?);
        
    }

    let response = fetch_with_retries(client, &format!("{}{}.json", PACKAGE_METADATA_URL, package_name), 3, headers).await?;
    
    if response.status() == 304 {
        return Ok(());
    }
    
    let path_obj = Path::new(&path);
    if let Some(parent) = path_obj.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await?;

    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let data = chunk?;
        file.write_all(&data).await?;
    }

    Ok(())
}

fn create_packages_download_bar(count: u64) -> ProgressBar{
    let pb = ProgressBar::new(count);
    pb
}

fn create_url_load_bar(size: u64) -> ProgressBar{
    let pb = ProgressBar::new(size);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn FmtWrite| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));

    pb
}