//! TLS/mTLS termination with hot certificate rotation (P5-1, 蓝图 §4.3).
//!
//! Both protocol sides share this module: `TlsConfigStore` holds the current
//! `rustls::ServerConfig` behind an `ArcSwap`, and every new connection reads
//! its acceptor from the swap — that IS the "acceptor rebuild" the blueprint
//! calls for (tonic's `ServerTlsConfig` and axum-server's `RustlsConfig` are
//! static once serving starts, so neither can rotate; 评审 1.8's feasibility
//! escape hatch is answered by bypassing them while keeping tonic/axum-server
//! for everything else).
//!
//! Rotation triggers: SIGHUP (unix) or a content-hash poll (`spawn_cert_reloader`)
//! — the poll also covers k8s secret-volume symlink swaps, which per-file
//! `notify` watches miss. A failed reload (corrupt PEM, key/cert mismatch)
//! keeps serving the previous certificate and logs an error; the next poll
//! retries, so a partially-written rotation self-heals.
//!
//! Deliberate policy (蓝图评审 2.2): no CRL/OCSP revocation checks (documented
//! out-of-scope); ring is the only crypto provider in the build graph and every
//! config is built via `builder_with_provider`, so the `builder()` ambiguity
//! panic (评审 2.4) cannot occur.

use crate::config::TlsSettings;
use crate::error::AppError;
use arc_swap::ArcSwap;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

/// Which protocol a TLS listener terminates; drives the ALPN advertisement.
/// gRPC is HTTP/2-only; HTTP offers h2 with an http/1.1 fallback so probes and
/// simple clients (Prometheus scrape, curl) keep working (蓝图: 开 TLS 后探活
/// 与内部客户端走 https)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsProtocol {
    Http,
    Grpc,
}

/// TLS handshake budget per accepted connection — bounds slow-loris exposure
/// on the accept path.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

static TLS_VERSIONS_12_UP: &[&rustls::SupportedProtocolVersion] =
    &[&rustls::version::TLS12, &rustls::version::TLS13];
static TLS_VERSIONS_13_ONLY: &[&rustls::SupportedProtocolVersion] =
    &[&rustls::version::TLS13];

/// Atomic (hash + config) pair so a rotation never exposes a mismatched view.
struct TlsState {
    content_hash: u64,
    config: Arc<ServerConfig>,
}

/// The rotating TLS configuration for one listener side (HTTP or gRPC).
/// Cheap to clone (Arc inside) — the reload driver and the server acceptor
/// share one instance.
pub struct TlsConfigStore {
    settings: TlsSettings,
    protocol: TlsProtocol,
    /// K2: HTTP listener advertises only http/1.1 (ka <= 0). See
    /// load_with_h1_only.
    h1_only: bool,
    state: ArcSwap<TlsState>,
}

impl TlsConfigStore {
    /// Load and fully validate the PEM files at startup (fail fast): unreadable
    /// files, empty/invalid PEM, a key that does not match the certificate, or
    /// an mTLS CA bundle with no usable certificate are all startup errors.
    pub fn load(settings: &TlsSettings, protocol: TlsProtocol) -> Result<Self, AppError> {
        Self::load_with_h1_only(settings, protocol, false)
    }

    /// K2 (resource-leak-plan): `h1_only` restricts the HTTP ALPN
    /// advertisement to http/1.1 — the only honest way to disable keep-alive
    /// on the TLS path, since `Connection: close` has no h2 equivalent and
    /// hyper-util's auto builder sniffs h2 regardless of http1_only(). The
    /// flag is stored so cert rotation rebuilds keep the restriction.
    pub fn load_with_h1_only(
        settings: &TlsSettings,
        protocol: TlsProtocol,
        h1_only: bool,
    ) -> Result<Self, AppError> {
        let pem = read_pem_files(settings)?;
        let config = build_server_config(settings, protocol, h1_only, &pem)?;
        warn_if_key_world_readable(&settings.key_path);
        Ok(Self {
            settings: settings.clone(),
            protocol,
            h1_only,
            state: ArcSwap::from_pointee(TlsState {
                content_hash: pem.content_hash(),
                config: Arc::new(config),
            }),
        })
    }

    /// Acceptor snapshot for new connections. Cheap (Arc clone); each accepted
    /// connection takes a fresh snapshot so a rotation applies to the NEXT
    /// handshake while in-flight connections keep their negotiated session.
    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.state.load().config.clone())
    }

    /// Re-read the PEM files; rebuild and swap the config only when the
    /// content changed. Returns whether a rotation happened. On any error the
    /// previous configuration keeps serving (rotation is retried by the next
    /// poll/SIGHUP).
    pub fn reload(&self) -> Result<bool, AppError> {
        let pem = read_pem_files(&self.settings)?;
        let hash = pem.content_hash();
        if self.state.load().content_hash == hash {
            return Ok(false);
        }
        let config = build_server_config(&self.settings, self.protocol, self.h1_only, &pem)?;
        self.state.store(Arc::new(TlsState {
            content_hash: hash,
            config: Arc::new(config),
        }));
        Ok(true)
    }

    /// File group description for reload logs.
    pub fn describe(&self) -> String {
        format!("cert={}", self.settings.cert_path)
    }
}

struct PemFiles {
    cert: Vec<u8>,
    key: Vec<u8>,
    ca: Option<Vec<u8>>,
}

impl PemFiles {
    fn content_hash(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.cert.hash(&mut h);
        self.key.hash(&mut h);
        self.ca.hash(&mut h);
        h.finish()
    }
}

fn read_pem_files(settings: &TlsSettings) -> Result<PemFiles, AppError> {
    let read = |path: &str| {
        std::fs::read(path)
            .map_err(|e| AppError::Config(format!("failed to read TLS file {}: {}", path, e)))
    };
    Ok(PemFiles {
        cert: read(&settings.cert_path)?,
        key: read(&settings.key_path)?,
        ca: match &settings.mtls_ca_path {
            Some(p) => Some(read(p)?),
            None => None,
        },
    })
}

/// Warn (not fail) when the private key is group/world-readable — 0o600 is the
/// recommendation (蓝图评审 1.8), but group-readable deployments (0o640 with a
/// service group) are legitimate, so this stays a warning.
fn warn_if_key_world_readable(key_path: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(key_path) {
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                tracing::warn!(
                    "TLS private key {} has mode {:o} — group/world-readable; recommend chmod 0600",
                    key_path,
                    mode
                );
            }
        }
    }
    #[cfg(not(unix))]
    let _ = key_path;
}

fn build_server_config(
    settings: &TlsSettings,
    protocol: TlsProtocol,
    h1_only: bool,
    pem: &PemFiles,
) -> Result<ServerConfig, AppError> {
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &pem.cert[..])
        .collect::<Result<_, _>>()
        .map_err(|e| {
            AppError::Config(format!("failed to parse TLS certificate {}: {}", settings.cert_path, e))
        })?;
    if certs.is_empty() {
        return Err(AppError::Config(format!(
            "TLS certificate file {} contains no certificates",
            settings.cert_path
        )));
    }
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &pem.key[..])
        .map_err(|e| {
            AppError::Config(format!("failed to parse TLS private key {}: {}", settings.key_path, e))
        })?
        .ok_or_else(|| {
            AppError::Config(format!(
                "TLS private key file {} contains no private key",
                settings.key_path
            ))
        })?;

    let versions = match settings.min_version.as_str() {
        "1.3" => TLS_VERSIONS_13_ONLY,
        _ => TLS_VERSIONS_12_UP, // values gated by Config::validate
    };
    let builder = ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_protocol_versions(versions)
        .map_err(|e| AppError::Config(format!("TLS protocol version setup failed: {}", e)))?;

    let builder = match &pem.ca {
        Some(ca_pem) => {
            let mut roots = RootCertStore::empty();
            let mut added = 0usize;
            for cert in rustls_pemfile::certs(&mut &ca_pem[..]) {
                let cert = cert.map_err(|e| {
                    AppError::Config(format!("failed to parse mTLS CA bundle: {}", e))
                })?;
                roots.add(cert).map_err(|e| {
                    AppError::Config(format!("invalid CA certificate in mTLS bundle: {}", e))
                })?;
                added += 1;
            }
            if added == 0 {
                return Err(AppError::Config(
                    "mTLS CA bundle contains no certificates".to_string(),
                ));
            }
            // mTLS: a client certificate is REQUIRED. webpki verifies chain +
            // clientAuth EKU against these roots; CRL/OCSP is out of scope
            // (蓝图评审 2.2).
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| AppError::Config(format!("failed to build mTLS verifier: {}", e)))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };

    // with_single_cert also rejects a key that does not match the leaf cert.
    let mut config = builder.with_single_cert(certs, key).map_err(|e| {
        AppError::Config(format!(
            "TLS certificate/key load failed ({} / {}): {}",
            settings.cert_path, settings.key_path, e
        ))
    })?;
    config.alpn_protocols = match protocol {
        TlsProtocol::Grpc => vec![b"h2".to_vec()],
        // K2: h1_only (ka <= 0) drops the h2 offer entirely.
        TlsProtocol::Http if h1_only => vec![b"http/1.1".to_vec()],
        TlsProtocol::Http => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
    };
    Ok(config)
}

/// mTLS client principal (蓝图评审 2.2 → T1 `RequestContext.principal`):
/// URI SAN (SPIFFE-style) first, then DNS SAN, then the subject DN; a cert
/// that fails to parse falls back to its SHA-256 fingerprint. The certificate
/// was already chain-verified by the TLS handshake — this is extraction only.
pub fn principal_from_cert(der: &CertificateDer<'_>) -> String {
    use x509_parser::nom::AsBytes;
    if let Ok((_, cert)) = x509_parser::parse_x509_certificate(der.as_bytes()) {
        if let Ok(Some(san)) = cert.subject_alternative_name() {
            let mut dns = None;
            for name in &san.value.general_names {
                match name {
                    x509_parser::extensions::GeneralName::URI(uri) => return uri.to_string(),
                    x509_parser::extensions::GeneralName::DNSName(d) if dns.is_none() => {
                        dns = Some(d.to_string())
                    }
                    _ => {}
                }
            }
            if let Some(dns) = dns {
                return dns;
            }
        }
        let dn = cert.subject().to_string();
        if !dn.is_empty() {
            return dn;
        }
    }
    sha256_fingerprint(der.as_bytes())
}

fn sha256_fingerprint(der: &[u8]) -> String {
    use sha2::Digest;
    use std::fmt::Write as _;
    let digest = sha2::Sha256::digest(der);
    let mut s = String::with_capacity(7 + 64);
    s.push_str("sha256:");
    for b in digest {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// mTLS client principal as an HTTP request extension (P5-1): inserted by
/// `RotatingTlsAcceptor`'s service wrapper, consumed by
/// `RequestContext::from_http_parts` → T1 `principal`. `None` on one-way TLS.
#[derive(Clone, Debug)]
pub struct TlsClientPrincipal(pub Option<String>);

/// axum-server `Accept` that terminates TLS with the store's current config
/// and tags each connection's service with the client principal.
#[derive(Clone)]
pub struct RotatingTlsAcceptor {
    store: Arc<TlsConfigStore>,
    /// RN-1: bound concurrent handshakes — the gRPC side (tls_incoming below)
    /// has had this gate since P5-1; the HTTP side was missing it, leaving a
    /// slow-loris opening (one spawned task + TCP connection per attempt,
    /// unbounded).
    handshake_permits: Arc<Semaphore>,
    /// D7: hard connection cap (0 = off). The counter is incremented at
    /// accept and decremented by CountedTlsStream's Drop.
    max_connections: usize,
    open_connections: Arc<std::sync::atomic::AtomicUsize>,
}

/// RN-1: matches the gRPC-side gate in tls_incoming.
const MAX_CONCURRENT_TLS_HANDSHAKES: usize = 1024;

impl RotatingTlsAcceptor {
    pub fn new(store: Arc<TlsConfigStore>) -> Self {
        Self::with_handshake_limit(store, MAX_CONCURRENT_TLS_HANDSHAKES)
    }

    /// RN-1 test hook: a custom limit so gate exhaustion is observable
    /// without opening 1024 connections.
    pub fn with_handshake_limit(store: Arc<TlsConfigStore>, limit: usize) -> Self {
        Self {
            store,
            handshake_permits: Arc::new(Semaphore::new(limit)),
            max_connections: 0,
            open_connections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// D7: install the hard connection cap (server.max_connections; 0 = off).
    pub fn with_connection_limit(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }
}

/// L4: wraps the post-handshake TLS stream so the
/// `liteserver_http_connections{transport="tls"}` gauge is held for exactly
/// the connection's lifetime (inc at accept, dec when hyper drops the stream
/// at connection end). Pure delegation otherwise. Unpin: TlsStream<TcpStream>
/// is Unpin, so plain field access suffices (no pin-project — it forbids a
/// Drop impl).
pub struct CountedTlsStream {
    inner: tokio_rustls::server::TlsStream<TcpStream>,
    /// D7: the acceptor's open-connection counter (decremented on drop).
    open_connections: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountedTlsStream {
    fn new(
        inner: tokio_rustls::server::TlsStream<TcpStream>,
        open_connections: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        crate::metrics::prometheus::record_http_connection_open("tls");
        Self {
            inner,
            open_connections,
        }
    }
}

impl Unpin for CountedTlsStream {}

/// D7 leak fix: compensates the accept-time `open_connections.fetch_add`
/// unless disarmed. CountedTlsStream — the usual decrement owner — is only
/// constructed after a successful handshake, so every failure branch
/// (handshake permit refused / handshake error / handshake timeout) used to
/// leak a permanent +1 and drift the D7 cap counter upward until
/// max_connections refused ALL TLS connections. The guard is created right
/// after the increment, and disarmed only when CountedTlsStream takes over
/// the decrement duty.
struct ConnectionCountGuard {
    open_connections: Arc<std::sync::atomic::AtomicUsize>,
    armed: bool,
}

impl ConnectionCountGuard {
    fn new(open_connections: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self {
            open_connections,
            armed: true,
        }
    }

    /// Hand over the decrement duty to CountedTlsStream (handshake succeeded).
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ConnectionCountGuard {
    fn drop(&mut self) {
        if self.armed {
            self.open_connections
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

impl Drop for CountedTlsStream {
    fn drop(&mut self) {
        crate::metrics::prometheus::record_http_connection_close("tls");
        self.open_connections
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

impl tokio::io::AsyncRead for CountedTlsStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for CountedTlsStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl<S> axum_server::accept::Accept<TcpStream, S> for RotatingTlsAcceptor
where
    S: Clone + Send + 'static,
{
    type Stream = CountedTlsStream;
    type Service = InjectPrincipal<S>;
    type Future =
        std::pin::Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let acceptor = self.store.acceptor();
        // D7: refuse over-cap connections at accept (no channel to answer on
        // before the connection exists).
        if self.max_connections > 0
            && self.open_connections.load(std::sync::atomic::Ordering::Acquire)
                >= self.max_connections
        {
            return Box::pin(async {
                Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "max_connections reached",
                ))
            });
        }
        self.open_connections
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        // D7 leak fix: any early return from here on (permit refusal,
        // handshake error/timeout, dropped future) must return the +1.
        let guard = ConnectionCountGuard::new(self.open_connections.clone());
        let open_connections = self.open_connections.clone();
        // L5: OS-level keepalive on the accepted socket (plaintext path
        // parity — serve_tcp does the same).
        crate::http::set_tcp_keepalive(&stream);
        // RN-1: refuse over-limit handshakes immediately (try_acquire, never
        // queue) — queueing would hold the TCP connection + task unbounded,
        // which is exactly the resource this gate protects.
        let permit = match self.handshake_permits.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                return Box::pin(async {
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        "too many concurrent TLS handshakes",
                    ))
                });
            }
        };
        Box::pin(async move {
            let _permit = permit; // held for the handshake duration
            let tls = match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
            {
                Ok(res) => res?,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TLS handshake timed out",
                    ))
                }
            };
            let principal = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
                .map(principal_from_cert);
            // Handshake succeeded: CountedTlsStream owns the decrement from
            // here (its Drop), so the guard must stand down.
            guard.disarm();
            Ok((CountedTlsStream::new(tls, open_connections), InjectPrincipal { inner: service, principal }))
        })
    }
}

/// Per-connection service wrapper that stamps the mTLS principal onto every
/// request's extensions (the accept-time handshake result is the only source;
/// headers must never set this).
#[derive(Clone)]
pub struct InjectPrincipal<S> {
    inner: S,
    principal: Option<String>,
}

impl<S, ReqBody> tower_service::Service<axum::http::Request<ReqBody>> for InjectPrincipal<S>
where
    S: tower_service::Service<axum::http::Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: axum::http::Request<ReqBody>) -> Self::Future {
        req.extensions_mut()
            .insert(TlsClientPrincipal(self.principal.clone()));
        self.inner.call(req)
    }
}

/// gRPC-side TLS incoming stream (P5-1): accepts TCP, runs each handshake in
/// its own task (a stalled handshake never blocks new accepts), and yields
/// post-handshake `TlsStream<TcpStream>` items. tonic's blanket
/// `Connected for TlsStream<TcpStream>` impl then puts `TlsConnectInfo`
/// (remote addr + peer certs) into every request's extensions, so
/// `Request::remote_addr()` and the mTLS principal work unchanged.
///
/// The accept loop exits on `stop` (the server's shutdown signal) or when the
/// receiving end is dropped.
pub fn tls_incoming(
    listener: tokio::net::TcpListener,
    store: Arc<TlsConfigStore>,
    stop: impl Future<Output = ()> + Send + 'static,
) -> impl tokio_stream::Stream<Item = io::Result<tokio_rustls::server::TlsStream<TcpStream>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    // Bound concurrent TLS handshakes so a slow-loris-style attacker cannot
    // pin unbounded tasks/connections in the handshake stage (the per-
    // connection `TLS_HANDSHAKE_TIMEOUT` bounds each one; this caps how many
    // run at once). The permit is acquired BEFORE accept so both in-flight
    // connections and handshakes are capped; it is held only for the handshake
    // duration and released when the spawned task ends.
    const MAX_CONCURRENT_TLS_HANDSHAKES: usize = 1024;
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_TLS_HANDSHAKES));
    tokio::spawn(async move {
        tokio::pin!(stop);
        loop {
            let permit = tokio::select! {
                biased;
                _ = &mut stop => break,
                p = permits.clone().acquire_owned() => match p {
                    Ok(p) => p,
                    // Semaphore never closes (its Arc outlives the loop).
                    Err(_) => break,
                },
            };
            let accepted = tokio::select! {
                res = listener.accept() => res,
                _ = &mut stop => break,
            };
            let (stream, _peer) = match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    // Transient accept failure (e.g. EMFILE): brief backoff,
                    // keep the listener alive (mirrors tonic's continue-on-
                    // transient accept-error policy). Release the permit first
                    // so the backoff doesn't hold a handshake slot.
                    tracing::debug!("TLS accept error: {}", e);
                    drop(permit);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let acceptor = store.acceptor();
            let tx = tx.clone();
            tokio::spawn(async move {
                let _permit = permit; // released when the task ends
                let result = match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(res) => res,
                    Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out")),
                };
                match result {
                    Ok(tls) => {
                        // Receiver gone → server is shutting down.
                        let _ = tx.send(Ok(tls)).await;
                    }
                    Err(e) => {
                        tracing::debug!("TLS handshake failed: {}", e);
                    }
                }
            });
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Reload driver (P5-1, 蓝图 D28): fires on SIGHUP (unix) and on a fixed
/// content-hash poll. The poll reads through symlinks, so k8s secret-volume
/// rotations (atomic symlink swap) are covered without `notify`. Rotation
/// applies to new connections only; established sessions are untouched.
pub fn spawn_cert_reloader(stores: Vec<Arc<TlsConfigStore>>, poll_interval: Duration) {
    if stores.is_empty() {
        return;
    }
    tokio::spawn(async move {
        #[cfg(unix)]
        let mut sighup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!("failed to install SIGHUP handler; TLS reload is poll-only: {}", e);
                None
            }
        };
        let mut tick = tokio::time::interval(poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            #[cfg(unix)]
            tokio::select! {
                _ = tick.tick() => {}
                _ = async {
                    match sighup.as_mut() {
                        Some(s) => s.recv().await,
                        None => std::future::pending::<Option<()>>().await,
                    }
                } => {
                    tracing::info!("SIGHUP received, reloading TLS certificates");
                }
            }
            #[cfg(not(unix))]
            tick.tick().await;

            for store in &stores {
                match store.reload() {
                    Ok(true) => tracing::info!("TLS certificate rotated ({})", store.describe()),
                    Ok(false) => {}
                    Err(e) => tracing::error!(
                        "TLS certificate reload failed for {} — keeping previous certificate: {}",
                        store.describe(),
                        e
                    ),
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::ServerName;
    use std::net::{IpAddr, Ipv4Addr};
    use tokio_rustls::TlsConnector;

    // ----- rcgen test PKI -----

    struct TestPki {
        ca_cert: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
        ca_pem: String,
    }

    impl TestPki {
        fn new(cn: &str) -> Self {
            let mut params = rcgen::CertificateParams::default();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, cn);
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = params.self_signed(&key).unwrap();
            Self {
                ca_pem: cert.pem(),
                ca_cert: cert,
                ca_key: key,
            }
        }

        fn sign_server(&self) -> (String, String) {
            let mut params =
                rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
            params.subject_alt_names = vec![
                rcgen::SanType::DnsName("localhost".try_into().unwrap()),
                rcgen::SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            ];
            params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &self.ca_cert, &self.ca_key).unwrap();
            (cert.pem(), key.serialize_pem())
        }

        fn sign_client(&self, sans: Vec<rcgen::SanType>, cn: Option<&str>) -> (String, String) {
            let mut params = rcgen::CertificateParams::default();
            params.subject_alt_names = sans;
            if let Some(cn) = cn {
                params.distinguished_name.push(rcgen::DnType::CommonName, cn);
            }
            params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &self.ca_cert, &self.ca_key).unwrap();
            (cert.pem(), key.serialize_pem())
        }
    }

    fn write_file(dir: &std::path::Path, name: &str, content: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        path.to_string_lossy().to_string()
    }

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "lite-server-tls-test-{}-{}",
            tag,
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn settings(cert: &str, key: &str, ca: Option<&str>) -> TlsSettings {
        TlsSettings {
            cert_path: cert.to_string(),
            key_path: key.to_string(),
            mtls_ca_path: ca.map(String::from),
            min_version: "1.2".to_string(),
        }
    }

    /// Set up a store + temp files; returns (store, paths of cert/key files).
    fn store_for(
        pki: &TestPki,
        protocol: TlsProtocol,
        mtls: bool,
        tag: &str,
    ) -> (Arc<TlsConfigStore>, std::path::PathBuf) {
        let dir = tempdir(tag);
        let (cert_pem, key_pem) = pki.sign_server();
        let cert = write_file(&dir, "server.crt", &cert_pem);
        let key = write_file(&dir, "server.key", &key_pem);
        let ca = if mtls {
            Some(write_file(&dir, "ca.crt", &pki.ca_pem))
        } else {
            None
        };
        let s = settings(&cert, &key, ca.as_deref());
        let store = TlsConfigStore::load(&s, protocol).expect("store load");
        (Arc::new(store), dir)
    }

    fn client_config(pki: &TestPki, client: Option<(String, String)>) -> rustls::ClientConfig {
        client_config_versions(pki, client, TLS_VERSIONS_12_UP)
    }

    fn client_config_versions(
        pki: &TestPki,
        client: Option<(String, String)>,
        versions: &[&'static rustls::SupportedProtocolVersion],
    ) -> rustls::ClientConfig {
        let mut roots = RootCertStore::empty();
        roots
            .add(
                rustls_pemfile::certs(&mut pki.ca_pem.as_bytes())
                    .next()
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let builder =
            rustls::ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
                .with_protocol_versions(versions)
                .unwrap()
                .with_root_certificates(roots);
        match client {
            Some((cert_pem, key_pem)) => {
                let certs: Vec<_> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
                    .collect::<Result<_, _>>()
                    .unwrap();
                let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
                    .unwrap()
                    .unwrap();
                builder.with_client_auth_cert(certs, key).unwrap()
            }
            None => builder.with_no_client_auth(),
        }
    }

    /// Run one handshake: server accepts once on an ephemeral loopback port,
    /// client connects with `server_name`. Returns (server result, client result).
    async fn run_handshake(
        store: Arc<TlsConfigStore>,
        client_config: rustls::ClientConfig,
        server_name: &str,
    ) -> (
        io::Result<tokio_rustls::server::TlsStream<TcpStream>>,
        io::Result<tokio_rustls::client::TlsStream<TcpStream>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            store.acceptor().accept(stream).await
        });
        let connector = TlsConnector::from(Arc::new(client_config));
        let client_stream = TcpStream::connect(addr).await.unwrap();
        let client_result = connector
            .connect(ServerName::try_from(server_name.to_string()).unwrap(), client_stream)
            .await;
        let server_result = server_task.await.unwrap();
        (server_result, client_result)
    }

    /// The certificate the *server* presented, as seen by the client.
    fn server_cert_seen_by_client(
        tls: &tokio_rustls::client::TlsStream<TcpStream>,
    ) -> Option<CertificateDer<'static>> {
        tls.get_ref().1.peer_certificates().and_then(|c| c.first()).cloned()
    }

    /// The certificate the *client* presented, as seen by the server (mTLS).
    fn client_cert_seen_by_server(
        tls: &tokio_rustls::server::TlsStream<TcpStream>,
    ) -> Option<CertificateDer<'static>> {
        tls.get_ref().1.peer_certificates().and_then(|c| c.first()).cloned()
    }

    fn pem_first_der(pem: &str) -> CertificateDer<'static> {
        rustls_pemfile::certs(&mut pem.as_bytes()).next().unwrap().unwrap()
    }

    // ----- store load validation -----

    #[test]
    fn load_rejects_bad_cert_pem() {
        let dir = tempdir("badcert");
        let cert = write_file(&dir, "server.crt", "not a pem");
        let key = write_file(&dir, "server.key", "also not a pem");
        let err = TlsConfigStore::load(&settings(&cert, &key, None), TlsProtocol::Http)
            .err()
            .expect("load must fail");
        assert!(err.to_string().contains("no certificates"), "{err}");
    }

    #[test]
    fn load_rejects_mismatched_key() {
        let pki = TestPki::new("ca");
        let dir = tempdir("mismatch");
        let (cert_pem, _key) = pki.sign_server();
        let (_c2, key2) = pki.sign_server(); // different keypair
        let cert = write_file(&dir, "server.crt", &cert_pem);
        let key = write_file(&dir, "server.key", &key2);
        let err = TlsConfigStore::load(&settings(&cert, &key, None), TlsProtocol::Http)
            .err()
            .expect("load must fail");
        assert!(err.to_string().contains("certificate/key"), "{err}");
    }

    #[test]
    fn load_rejects_empty_ca_bundle() {
        let pki = TestPki::new("ca");
        let dir = tempdir("emptyca");
        let (cert_pem, key_pem) = pki.sign_server();
        let cert = write_file(&dir, "server.crt", &cert_pem);
        let key = write_file(&dir, "server.key", &key_pem);
        let ca = write_file(&dir, "ca.crt", "");
        let err = TlsConfigStore::load(&settings(&cert, &key, Some(&ca)), TlsProtocol::Http)
            .err()
            .expect("load must fail");
        assert!(err.to_string().contains("no certificates"), "{err}");
    }

    // ----- handshakes: one-way TLS / mTLS accept & reject -----

    #[tokio::test]
    async fn one_way_tls_handshake_works_with_h2_alpn() {
        let pki = TestPki::new("ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Http, false, "oneway");
        let mut cfg = client_config(&pki, None);
        cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let (server, client) = run_handshake(store, cfg, "localhost").await;
        let server = server.expect("server handshake");
        let client = client.expect("client handshake");
        assert_eq!(client.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
        assert!(client_cert_seen_by_server(&server).is_none(), "one-way TLS has no client cert");
    }

    #[tokio::test]
    async fn http_alpn_falls_back_to_http11_for_h1_only_clients() {
        let pki = TestPki::new("ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Http, false, "alpn11");
        let mut cfg = client_config(&pki, None);
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        let (server, client) = run_handshake(store, cfg, "localhost").await;
        server.expect("server handshake");
        let client = client.expect("client handshake");
        assert_eq!(client.get_ref().1.alpn_protocol(), Some(b"http/1.1".as_slice()));
    }

    #[tokio::test]
    async fn mtls_accepts_client_cert_signed_by_ca() {
        let pki = TestPki::new("ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Grpc, true, "mtlsok");
        let client_cert = pki.sign_client(
            vec![rcgen::SanType::URI("spiffe://ns/svc".try_into().unwrap())],
            None,
        );
        let (server, client) =
            run_handshake(store, client_config(&pki, Some(client_cert.clone())), "localhost").await;
        let server = server.expect("server handshake");
        client.expect("client handshake");
        let peer = client_cert_seen_by_server(&server).expect("client cert must be presented");
        assert_eq!(peer, pem_first_der(&client_cert.0));
    }

    #[tokio::test]
    async fn mtls_rejects_client_without_certificate() {
        // 蓝图测试项：mTLS 无客户端证书握手拒绝。
        let pki = TestPki::new("ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Grpc, true, "mtlsno");
        let (server, _client) = run_handshake(store, client_config(&pki, None), "localhost").await;
        assert!(server.is_err(), "server must abort when the client sends no certificate");
    }

    #[tokio::test]
    async fn mtls_rejects_client_cert_from_foreign_ca() {
        let pki = TestPki::new("ca");
        let foreign = TestPki::new("foreign-ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Http, true, "mtlsbad");
        let foreign_client = foreign.sign_client(
            vec![rcgen::SanType::DnsName("evil".try_into().unwrap())],
            None,
        );
        // Client trusts our CA (so it completes its side), but presents a cert
        // signed by a foreign CA → server must reject.
        let (server, _client) =
            run_handshake(store, client_config(&pki, Some(foreign_client)), "localhost").await;
        assert!(server.is_err(), "server must reject a client cert from an unknown CA");
    }

    #[tokio::test]
    async fn tls13_only_store_rejects_tls12_client() {
        let pki = TestPki::new("ca");
        let dir = tempdir("tls13");
        let (cert_pem, key_pem) = pki.sign_server();
        let cert = write_file(&dir, "server.crt", &cert_pem);
        let key = write_file(&dir, "server.key", &key_pem);
        let mut s = settings(&cert, &key, None);
        s.min_version = "1.3".to_string();
        let store = Arc::new(TlsConfigStore::load(&s, TlsProtocol::Grpc).unwrap());

        // TLS 1.2-only client → version alert.
        let cfg12 = client_config_versions(&pki, None, &[&rustls::version::TLS12]);
        let (server, _c) = run_handshake(store.clone(), cfg12, "localhost").await;
        assert!(server.is_err(), "TLS 1.2 client must be rejected by a 1.3-only server");

        // TLS 1.3 client → ok.
        let cfg13 = client_config_versions(&pki, None, &[&rustls::version::TLS13]);
        let (server, client) = run_handshake(store, cfg13, "localhost").await;
        server.expect("1.3 server handshake");
        client.expect("1.3 client handshake");
    }

    // ----- principal extraction -----

    #[test]
    fn principal_prefers_uri_san() {
        let pki = TestPki::new("ca");
        let (cert_pem, _) = pki.sign_client(
            vec![
                rcgen::SanType::DnsName("fallback.example".try_into().unwrap()),
                rcgen::SanType::URI("spiffe://ns/svc".try_into().unwrap()),
            ],
            Some("cn-client"),
        );
        assert_eq!(principal_from_cert(&pem_first_der(&cert_pem)), "spiffe://ns/svc");
    }

    #[test]
    fn principal_falls_back_to_dns_san_then_dn() {
        let pki = TestPki::new("ca");
        let (dns_cert, _) = pki.sign_client(
            vec![rcgen::SanType::DnsName("client.example".try_into().unwrap())],
            Some("cn-client"),
        );
        assert_eq!(principal_from_cert(&pem_first_der(&dns_cert)), "client.example");

        let (dn_cert, _) = pki.sign_client(vec![], Some("dn-client"));
        let principal = principal_from_cert(&pem_first_der(&dn_cert));
        assert!(principal.contains("CN=dn-client"), "DN principal: {principal}");
    }

    // ----- hot rotation -----

    #[tokio::test]
    async fn reload_rotates_certificate_for_new_connections() {
        let pki = TestPki::new("ca");
        let dir = tempdir("rotate");
        let (cert1_pem, key1_pem) = pki.sign_server();
        let cert = write_file(&dir, "server.crt", &cert1_pem);
        let key = write_file(&dir, "server.key", &key1_pem);
        let store = Arc::new(TlsConfigStore::load(&settings(&cert, &key, None), TlsProtocol::Http).unwrap());

        // Unchanged files → no-op.
        assert!(!store.reload().unwrap());

        let (_, client) = run_handshake(store.clone(), client_config(&pki, None), "localhost").await;
        assert_eq!(
            server_cert_seen_by_client(&client.unwrap()),
            Some(pem_first_der(&cert1_pem)),
            "first handshake serves cert1"
        );

        // Swap in cert2 → reload rotates; new handshakes see cert2.
        let (cert2_pem, key2_pem) = pki.sign_server();
        std::fs::write(&cert, &cert2_pem).unwrap();
        std::fs::write(&key, &key2_pem).unwrap();
        assert!(store.reload().unwrap(), "changed files must rotate");

        let (_, client) = run_handshake(store.clone(), client_config(&pki, None), "localhost").await;
        assert_eq!(
            server_cert_seen_by_client(&client.unwrap()),
            Some(pem_first_der(&cert2_pem)),
            "post-reload handshake must serve cert2"
        );
    }

    #[tokio::test]
    async fn failed_reload_keeps_previous_certificate() {
        let pki = TestPki::new("ca");
        let dir = tempdir("reloadfail");
        let (cert_pem, key_pem) = pki.sign_server();
        let cert = write_file(&dir, "server.crt", &cert_pem);
        let key = write_file(&dir, "server.key", &key_pem);
        let store = Arc::new(TlsConfigStore::load(&settings(&cert, &key, None), TlsProtocol::Http).unwrap());

        // Corrupt the cert file mid-rotation → reload errors, old cert serves.
        std::fs::write(&cert, "garbage").unwrap();
        assert!(store.reload().is_err());
        let (_, client) = run_handshake(store, client_config(&pki, None), "localhost").await;
        assert_eq!(
            server_cert_seen_by_client(&client.unwrap()),
            Some(pem_first_der(&cert_pem)),
            "failed reload must keep the previous certificate"
        );
    }

    // ----- tls_incoming -----

    #[tokio::test]
    async fn tls_incoming_yields_handshaken_streams_and_stops() {
        use tokio_stream::StreamExt;
        let pki = TestPki::new("ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Grpc, false, "incoming");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let mut incoming = Box::pin(tls_incoming(listener, store, async {
            let _ = stop_rx.await;
        }));

        let connector = TlsConnector::from(Arc::new(client_config(&pki, None)));
        let client = TcpStream::connect(addr).await.unwrap();
        let client_task = tokio::spawn(async move {
            connector
                .connect(ServerName::try_from("localhost".to_string()).unwrap(), client)
                .await
        });
        let server_tls = incoming
            .next()
            .await
            .expect("one connection")
            .expect("handshake ok");
        assert_eq!(
            server_tls.get_ref().1.alpn_protocol(),
            None,
            "client offered no ALPN in this test"
        );
        client_task.await.unwrap().expect("client handshake");
        let _ = stop_tx.send(());
    }

    /// K2 (resource-leak-plan): h1_only must restrict the HTTP ALPN
    /// advertisement to http/1.1 (ka <= 0 on the TLS path); the default keeps
    /// the h2 + http/1.1 offer.
    #[test]
    fn k2_h1_only_alpn_advertises_only_http11() {
        let pki = TestPki::new("k2-ca");
        let (cert_pem, key_pem) = pki.sign_server();
        let pem = PemFiles {
            cert: cert_pem.into_bytes(),
            key: key_pem.into_bytes(),
            ca: None,
        };
        // cert/key paths only appear in error messages here.
        let s = settings("cert", "key", None);
        let cfg = build_server_config(&s, TlsProtocol::Http, true, &pem).expect("config");
        assert_eq!(cfg.alpn_protocols, vec![b"http/1.1".to_vec()]);
        let cfg = build_server_config(&s, TlsProtocol::Http, false, &pem).expect("config");
        assert_eq!(
            cfg.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    /// RN-1 (resource-leak-plan): the HTTP TLS acceptor must bound concurrent
    /// handshakes (slow-loris gate; the gRPC side has had one since P5-1).
    /// With the limit exhausted, a new accept must be refused IMMEDIATELY —
    /// queueing would hold the connection + task unbounded, the exact
    /// resource the gate protects.
    #[tokio::test]
    async fn test_rn1_http_tls_handshake_gate_refuses_over_limit() {
        use axum_server::accept::Accept as _;

        let pki = TestPki::new("rn1-ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Http, false, "rn1");
        let acceptor = RotatingTlsAcceptor::with_handshake_limit(store, 1);

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let c1 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (s1, _) = listener.accept().await.unwrap();
        let c2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (s2, _) = listener.accept().await.unwrap();

        // The first accept takes the only permit synchronously (held for the
        // handshake duration; the client never speaks TLS, so the handshake
        // stalls inside the future).
        let first = acceptor.accept(s1, ());
        // The second must be refused immediately — not queued.
        let refused = match tokio::time::timeout(
            std::time::Duration::from_millis(500),
            acceptor.accept(s2, ()),
        )
        .await
        .expect("gate refusal must be immediate")
        {
            Ok(_) => panic!("over-limit handshake must be refused"),
            Err(e) => e,
        };
        assert_eq!(refused.kind(), std::io::ErrorKind::ConnectionRefused);
        assert!(
            refused.to_string().contains("too many concurrent TLS handshakes"),
            "unexpected refusal: {refused}"
        );
        drop(first);
        drop(c1);
        drop(c2);
    }

    /// D7 leak fix: the open-connection counter is incremented at accept but
    /// was only decremented by CountedTlsStream's Drop — which is constructed
    /// solely on handshake success. Every refused/failed/timed-out handshake
    /// leaked a permanent +1, drifting the D7 cap counter upward until
    /// max_connections refused ALL TLS connections. These three tests pin the
    /// compensating decrement on each failure branch.
    #[tokio::test]
    async fn d7_counter_released_when_handshake_gate_refuses() {
        use axum_server::accept::Accept as _;
        use std::sync::atomic::Ordering;

        let pki = TestPki::new("d7-gate-ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Http, false, "d7-gate");
        let acceptor = RotatingTlsAcceptor::with_handshake_limit(store, 1);

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let c1 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (s1, _) = listener.accept().await.unwrap();
        let c2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (s2, _) = listener.accept().await.unwrap();

        // First accept takes the only permit and stalls (client never speaks
        // TLS); its +1 is legitimately held for the test's duration.
        let first = acceptor.accept(s1, ());
        assert_eq!(acceptor.open_connections.load(Ordering::Acquire), 1);

        // The refused accept must return its +1 immediately.
        let refused = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            acceptor.accept(s2, ()),
        )
        .await
        .expect("gate refusal must be immediate");
        assert!(refused.is_err());
        assert_eq!(
            acceptor.open_connections.load(Ordering::Acquire),
            1,
            "refused handshake must not leak the D7 counter"
        );

        drop(first);
        drop(c1);
        drop(c2);
    }

    #[tokio::test]
    async fn d7_counter_released_when_handshake_fails() {
        use axum_server::accept::Accept as _;
        use std::sync::atomic::Ordering;

        let pki = TestPki::new("d7-fail-ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Http, false, "d7-fail");
        let acceptor = RotatingTlsAcceptor::new(store);

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        // Client drops immediately → rustls accept errors out fast (EOF).
        drop(client);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            acceptor.accept(server, ()),
        )
        .await
        .expect("failed handshake must resolve promptly");
        assert!(result.is_err());
        assert_eq!(
            acceptor.open_connections.load(Ordering::Acquire),
            0,
            "failed handshake must not leak the D7 counter"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn d7_counter_released_when_handshake_times_out() {
        use axum_server::accept::Accept as _;
        use std::sync::atomic::Ordering;

        let pki = TestPki::new("d7-timeout-ca");
        let (store, _dir) = store_for(&pki, TlsProtocol::Http, false, "d7-timeout");
        let acceptor = RotatingTlsAcceptor::new(store);

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Client connects but never speaks TLS; paused time auto-advances to
        // the TLS_HANDSHAKE_TIMEOUT timer once the runtime idles.
        let _client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        let result = acceptor.accept(server, ()).await;
        let err = match result {
            Ok(_) => panic!("stalled handshake must time out"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(
            acceptor.open_connections.load(Ordering::Acquire),
            0,
            "timed-out handshake must not leak the D7 counter"
        );
    }
}
