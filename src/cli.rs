use clap::Parser;
use tonic::metadata::AsciiMetadataValue;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLI {
    #[arg(long, value_name = "URL")]
    pub geyser_url: String,
    #[arg(long, value_name = "TOKEN")]
    pub geyser_x_token: Option<AsciiMetadataValue>,
    #[arg(long, value_name = "TOKEN")]
    pub geyser_x_access_token: Option<AsciiMetadataValue>,
    #[arg(long)]
    pub with_votes: bool,
    #[arg(long, default_value = "3000")]
    pub port: u16,
    #[arg(long, value_name = "N")]
    pub mapping_threads: Option<usize>
}