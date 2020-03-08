//! The intention is a general S3 cli.
use clap::{App, Arg, SubCommand};
use regex::Regex;
use rusoto_core::{credential::ProfileProvider, region::Region, HttpClient};
use s3_algo::*;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(PartialEq, Eq, Debug, Clone)]
enum Location {
    S3 {
        host: Option<String>,
        bucket: String,
        key: String,
    },
    Path(PathBuf),
}
impl From<&str> for Location {
    fn from(val: &str) -> Location {
        let re_s3 = Regex::new(r"s3://(?P<bucket>[^/]*)(/(?P<key>.*))?").unwrap();
        let re_endpoint =
            Regex::new(r"(?P<host>[^/:]*:[0-9]*)/(?P<bucket>[^/]*)(/(?P<key>.*))?").unwrap();
        if re_s3.is_match(val) {
            let cap = re_s3.captures_iter(val).next().unwrap();
            Location::S3 {
                host: None,
                bucket: cap["bucket"].to_string(),
                key: cap
                    .name("key")
                    .map(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
            }
        } else if re_endpoint.is_match(val) {
            let cap = re_endpoint.captures_iter(val).next().unwrap();
            Location::S3 {
                host: Some(cap["host"].to_string()),
                bucket: cap["bucket"].to_string(),
                key: cap
                    .name("key")
                    .map(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
            }
        } else {
            Location::Path(PathBuf::from(val))
        }
    }
}

#[tokio::main]
async fn main() {
    // TODO: Make it more useful.
    // So far we only allow copying from disk to S3, with a very limited set of options

    let matches = App::new("s3")
        .subcommand(
            SubCommand::with_name("cp")
                .arg(Arg::with_name("src").required(true))
                .arg(Arg::with_name("dest").required(true))
                .arg(
                    Arg::with_name("profile")
                        .long("profile")
                        .required(true)
                        .takes_value(true)
                        .help("Set profile"),
                )
                .arg(
                    Arg::with_name("sse")
                        .long("sse")
                        .help("Enable SSE")
                        .takes_value(false),
                )
                .arg(
                    Arg::with_name("region")
                        .long("region")
                        .help("AWS region. Default: eu-west-1")
                        .takes_value(true),
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("cp") {
        let src = Location::from(matches.value_of("src").unwrap());
        let dest = Location::from(matches.value_of("dest").unwrap());
        let profile = matches.value_of("profile").unwrap();
        let sse = matches.is_present("sse");
        let region = matches.value_of("region").unwrap_or("eu-west-1");

        let client = HttpClient::new().unwrap();
        let mut region = match region {
            "eu-west-1" => Region::EuWest1,
            "us-east-1" => Region::UsEast1,
            _ => unimplemented!(),
        };

        match (src, dest) {
            (Location::Path(path), Location::S3 { host, bucket, key }) => {
                let total_bytes = count_bytes_recursive(&path);
                let mut pr = ProfileProvider::new().unwrap();
                pr.set_profile(profile);

                let mut pb = pbr::ProgressBar::new(total_bytes);
                pb.set_units(pbr::Units::Bytes);
                let pb = Arc::new(Mutex::new(pb));
                if let Some(host) = host {
                    region = Region::Custom {
                        name: "custom".into(),
                        endpoint: format!("http://{}", host),
                    };
                }
                let cli = rusoto_s3::S3Client::new_with(client, pr, region);
                s3_upload_files(
                    cli,
                    bucket,
                    files_recursive(path, PathBuf::from(&key)),
                    UploadConfig::default(),
                    move |report| {
                        let pb = pb.clone();
                        async move {
                            // println!("Progress: {}/{}", report.bytes, total_bytes);
                            pb.lock().await.add(report.bytes);
                        }
                    },
                    Default::default,
                )
                .await
                .unwrap();
            }
            _ => unimplemented!(),
        }
    } else {
        unimplemented!()
    }
}

fn count_bytes_recursive(path: &Path) -> u64 {
    let mut bytes = 0;
    for file in walkdir::WalkDir::new(path).into_iter().filter_map(|entry| {
        entry.ok().and_then(|entry| {
            if entry.file_type().is_file() {
                Some(entry.path().to_owned())
            } else {
                None
            }
        })
    }) {
        bytes += file.metadata().unwrap().len();
    }
    return bytes;
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_location_parsing() {
        assert_eq!(
            Location::from("s3://a-b-c/abc"),
            Location::S3 {
                host: None,
                bucket: "a-b-c".into(),
                key: "abc".into()
            }
        );
        assert_eq!(
            Location::from("s3://a-b-c"),
            Location::S3 {
                host: None,
                bucket: "a-b-c".into(),
                key: String::new()
            }
        );
        assert_eq!(
            Location::from("s3://a-b-c/"),
            Location::S3 {
                host: None,
                bucket: "a-b-c".into(),
                key: String::new()
            }
        );
        assert_eq!(
            Location::from("s3://a-b-c/path/to/key"),
            Location::S3 {
                host: None,
                bucket: "a-b-c".into(),
                key: "path/to/key".into()
            }
        );
        assert_eq!(
            Location::from("anything/else"),
            Location::Path(PathBuf::from("anything/else"))
        );
        assert_eq!(
            Location::from("localhost:9000/testing/key"),
            Location::S3 {
                host: Some("localhost:9000".into()),
                bucket: "testing".into(),
                key: "key".into()
            }
        )
    }
}
