use tracing::subscriber;
pub fn enable_log() {
    enable_log_with_level(tracing::Level::INFO)
}
pub fn enable_log_with_level(level: tracing::Level) {
    let subscriber = tracing_subscriber::fmt().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
