// Differential fuzz target: the fast-path parser (parse_redis_value) must agree
// with pure `combine` (a fresh Parser bypasses the fast path on its empty
// buffer) on every input. Debug-string comparison so Double(NaN) compares equal.
use afl::fuzz;
use redis::{Parser, parse_redis_value};

fn main() {
    fuzz!(|data: &[u8]| {
        let fast = parse_redis_value(data);
        let combine = Parser::new().parse_value(data);
        match (&fast, &combine) {
            (Ok(a), Ok(b)) => {
                let (da, db) = (format!("{a:?}"), format!("{b:?}"));
                assert!(da == db, "VALUE DIVERGENCE\n fast={da}\n comb={db}\n in={data:?}");
            }
            (Err(_), Err(_)) => {}
            _ => panic!("DISPOSITION DIVERGENCE fast={fast:?} combine={combine:?} in={data:?}"),
        }
    });
}
