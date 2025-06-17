use tracing::subscriber;

pub fn enable_log_info() {
    enable_log(tracing::Level::INFO)
}
pub fn enable_log(level: tracing::Level) {
    let subscriber = tracing_subscriber::fmt().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
