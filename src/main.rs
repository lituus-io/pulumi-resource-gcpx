#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::net::SocketAddr;

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bind and print port FIRST — Pulumi reads port from stdout before connecting.
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    {
        use std::io::Write;
        let port = listener.local_addr()?.port();
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{port}")?;
        stdout.flush()?;
    }

    // Auth can be slow — do it after port is printed.
    let raw_token_source =
        pulumi_resource_gcpx::token_source::GcpAuthTokenSource(gcp_auth::provider().await?);
    // Wrap with caching: refresh 60s before estimated expiry.
    let token_source =
        pulumi_resource_gcpx::token_source::CachedTokenSource::new(raw_token_source, 60);
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let client = pulumi_resource_gcpx::gcp_client::HttpGcpClient::new(http, token_source);
    let provider = pulumi_resource_gcpx::provider::GcpxProvider::new(client);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    Server::builder()
        .add_service(pulumirpc::resource_provider_server::ResourceProviderServer::new(provider))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}
