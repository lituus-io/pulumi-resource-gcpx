use std::future::Future;
use std::sync::Arc;

/// Concrete boundary for `gcp_auth::TokenProvider` — contains `dyn` at
/// the edge so the rest of the codebase is fully monomorphized.
pub trait TokenSource: Send + Sync + 'static {
    fn token<'a>(
        &'a self,
        scopes: &'a [&'a str],
    ) -> impl Future<Output = Result<String, gcp_auth::Error>> + Send + 'a;
}

/// Production token source backed by `gcp_auth`.
pub struct GcpAuthTokenSource(pub Arc<dyn gcp_auth::TokenProvider>);

impl TokenSource for GcpAuthTokenSource {
    async fn token<'a>(&'a self, scopes: &'a [&'a str]) -> Result<String, gcp_auth::Error> {
        let token = self.0.token(scopes).await?;
        Ok(token.as_str().to_owned())
    }
}

/// Caching wrapper for any `TokenSource`. Returns cached token if not
/// expired (with configurable buffer before expiry), otherwise fetches
/// a new one through the inner source.
///
/// Generic over `T: TokenSource` — no dynamic dispatch, fully monomorphized.
pub struct CachedTokenSource<T: TokenSource> {
    inner: T,
    cached: tokio::sync::RwLock<Option<(String, std::time::Instant)>>,
    /// How many seconds before "expiry" to proactively refresh.
    buffer_secs: u64,
    /// Token lifetime assumption (gcp_auth tokens typically last 3600s).
    ttl_secs: u64,
}

impl<T: TokenSource> CachedTokenSource<T> {
    /// Wrap an inner token source with caching.
    ///
    /// `buffer_secs`: refresh this many seconds before estimated expiry.
    pub fn new(inner: T, buffer_secs: u64) -> Self {
        Self {
            inner,
            cached: tokio::sync::RwLock::new(None),
            buffer_secs,
            ttl_secs: 3600, // default 1h, matching GCP token lifetime
        }
    }
}

impl<T: TokenSource> TokenSource for CachedTokenSource<T> {
    async fn token<'a>(&'a self, scopes: &'a [&'a str]) -> Result<String, gcp_auth::Error> {
        // Fast path: read lock check.
        {
            let guard = self.cached.read().await;
            if let Some((ref tok, fetched_at)) = *guard {
                let elapsed = fetched_at.elapsed().as_secs();
                if elapsed + self.buffer_secs < self.ttl_secs {
                    return Ok(tok.clone());
                }
            }
        }

        // Slow path: write lock, double-check, then fetch.
        let mut guard = self.cached.write().await;
        // Double-check after acquiring write lock (another task may have refreshed).
        if let Some((ref tok, fetched_at)) = *guard {
            let elapsed = fetched_at.elapsed().as_secs();
            if elapsed + self.buffer_secs < self.ttl_secs {
                return Ok(tok.clone());
            }
        }

        let token = self.inner.token(scopes).await?;
        *guard = Some((token.clone(), std::time::Instant::now()));
        Ok(token)
    }
}

#[cfg(test)]
pub struct MockTokenSource;

#[cfg(test)]
impl TokenSource for MockTokenSource {
    async fn token<'a>(&'a self, _scopes: &'a [&'a str]) -> Result<String, gcp_auth::Error> {
        Ok("mock-token".to_owned())
    }
}
