//! Latency summary over recorded nanosecond samples.

pub struct Summary {
    pub n: usize,
    pub mean: f64,
    pub p50: u64,
    pub p99: u64,
    pub p9999: u64,
    pub max: u64,
}

impl Summary {
    pub fn of(samples: &mut [u64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let n = samples.len();
        let sum: u128 = samples.iter().map(|&v| v as u128).sum();
        let at = |q: f64| samples[(((n as f64) * q).ceil() as usize).clamp(1, n) - 1];
        Some(Self {
            n,
            mean: sum as f64 / n as f64,
            p50: at(0.50),
            p99: at(0.99),
            p9999: at(0.9999),
            max: samples[n - 1],
        })
    }
}

pub fn count_over(samples: &[u64], ns: u64) -> usize {
    samples.iter().filter(|&&v| v > ns).count()
}
