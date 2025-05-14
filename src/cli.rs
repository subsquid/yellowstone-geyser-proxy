use clap::Parser;
use tonic::metadata::AsciiMetadataValue;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLI {
    #[arg(long, value_name = "URL", help = "Yellowstone gRPC service")]
    pub geyser_url: String,
    #[arg(long, value_name = "TOKEN", help = "add X-Token header to gRPC requests")]
    pub geyser_x_token: Option<AsciiMetadataValue>,
    #[arg(long, value_name = "TOKEN", help = "add X-Access-Token header to gRPC requests")]
    pub geyser_x_access_token: Option<AsciiMetadataValue>,
    #[arg(long, help = "disable removal of vote transactions")]
    pub with_votes: bool,
    #[arg(long, default_value = "3000")]
    pub port: u16,
    #[arg(long, value_name = "N", help = "number of threads to use for data transformation")]
    pub mapping_threads: Option<usize>
}