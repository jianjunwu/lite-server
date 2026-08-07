//! P-XFF 受信代理 client-IP 清洗（蓝图 §4.3 P-XFF，fail-safe，peer 锚定）。
//!
//! 现状（修复前）的 `extract_client_ip` / `http_client_ip` 把客户端的
//! `X-Forwarded-For` 整串当真——任何直连客户端都能伪造 XFF，从而用一串假 IP
//! 绕过按 IP 限流（key="ip"）。本模块用 **peer 锚定**算法取首个不受信 hop，
//! 默认 `trusted_proxies` 为空 → 直接用 TCP peer、忽略所有 XFF/X-Real-IP。
//!
//! **算法（评审 1.2，peer 锚定为第一步）**:
//! 1. peer 存在且 ∉ trusted_proxies → **直接用 peer，忽略 XFF/X-Real-IP**
//!    （防直连伪造；这是修复前主路径漏写的核心安全步）。
//! 2. peer 存在且 ∈ trusted → XFF 从右向左首个非受信段 = 客户端；
//!    遇非法段即**终止**右移（不跳过）；全受信 → 取最左。
//! 3. peer 为 None（UDS 无 `ConnectInfo`，无锚点）→ 保留既有 header 优先语义
//!    （XFF > X-Real-IP），不引入新行为。
//! 4. 无可用 XFF → X-Real-IP（仅 peer 受信/无锚点时）→ peer → None。
//!
//! **fail-safe 默认**: trusted 空 → 用 peer（防伪造绕过限流）。CIDR 匹配用
//! `ipnet`（纯 Rust 跨平台）。loopback 判定与 P7-1 协同（用 peer 不用 XFF）。

use ipnet::IpNet;
use std::net::IpAddr;

/// `trusted_proxies` 条目解析后的受信网络集合（CIDR 或单 IP）。
pub type TrustedNetworks = Vec<IpNet>;

/// 解析单个 `trusted_proxies` 条目：先按 CIDR 解析，失败则按裸 IP 解析为
/// `/32`（v4）或 `/128`（v6）。两者皆失败 → None（调用方在启动期收集错误）。
pub fn parse_network(entry: &str) -> Option<IpNet> {
    if let Ok(net) = entry.trim().parse::<IpNet>() {
        return Some(net);
    }
    let ip = entry.trim().parse::<IpAddr>().ok()?;
    match ip {
        IpAddr::V4(v4) => Some(IpNet::V4(ipnet::Ipv4Net::new(v4, 32).ok()?)),
        IpAddr::V6(v6) => Some(IpNet::V6(ipnet::Ipv6Net::new(v6, 128).ok()?)),
    }
}

/// `ip` 是否落在任一受信 CIDR 段内（v4/v6 不匹配返回 false）。
fn is_trusted(ip: IpAddr, trusted: &[IpNet]) -> bool {
    trusted.iter().any(|net| net.contains(&ip))
}

/// 把（可能多个、按 RFC 出现序的）`X-Forwarded-For` 段合并成一条逗号分隔串。
/// 调用方负责按 header 出现序喂入，本函数仅用 `", "` 连接。
pub fn merge_xff(values: impl IntoIterator<Item = impl AsRef<str>>) -> Option<String> {
    let joined: Vec<String> = values
        .into_iter()
        .map(|v| v.as_ref().trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if joined.is_empty() {
        None
    } else {
        Some(joined.join(", "))
    }
}

/// 在 XFF 串上从右向左走，返回首个非受信段（= 客户端）：
/// - 遇受信段 → 继续左移；
/// - 遇非受信但合法的段 → 该段即客户端；
/// - 遇非法段 → **终止**（不信任更左的段），返回 None（fail-safe）；
/// - 走完整条链全受信 → 取最左段（best-guess 客户端）。
fn walk_xff(xff: &str, trusted: &[IpNet]) -> Option<IpAddr> {
    let segs: Vec<&str> = xff
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let mut client: Option<IpAddr> = None;
    let mut hit_invalid = false;
    for seg in segs.iter().rev() {
        match seg.parse::<IpAddr>() {
            Ok(ip) if is_trusted(ip, trusted) => continue,
            Ok(ip) => {
                client = Some(ip);
                break;
            }
            Err(_) => {
                hit_invalid = true;
                break;
            }
        }
    }
    if let Some(c) = client {
        return Some(c);
    }
    if hit_invalid {
        // 链中有非法段，终止右移——更左的段不可信，fail-safe 返回 None。
        return None;
    }
    // 整条链合法且全受信 → 最左段（best-guess 客户端）。
    segs.iter().filter_map(|s| s.parse::<IpAddr>().ok()).next()
}

/// 仅从 header 解析（不含 peer 兜底）：XFF > X-Real-IP。
fn resolve_from_headers(
    xff: Option<&str>,
    x_real_ip: Option<&str>,
    trusted: &[IpNet],
) -> Option<IpAddr> {
    if let Some(xff_str) = xff.map(str::trim).filter(|s| !s.is_empty()) {
        if let Some(ip) = walk_xff(xff_str, trusted) {
            return Some(ip);
        }
    }
    if let Some(xri) = x_real_ip.map(str::trim).filter(|s| !s.is_empty()) {
        if let Ok(ip) = xri.parse::<IpAddr>() {
            return Some(ip);
        }
    }
    None
}

/// 权威 client-IP 清洗入口（蓝图 P-XFF）。`xff`/`x_real_ip` 为合并后的整串
/// （重复 XFF 头由调用方按 RFC 出现序合并，见 [`merge_xff`]）。`peer` 为
/// 直连 TCP/UDS peer（None = 无 `ConnectInfo`，如 UDS）。
///
/// 返回 `Option<IpAddr>`：None = 无任何来源（UDS 无 peer 且无 header）。
pub fn extract_client_ip(
    xff: Option<&str>,
    x_real_ip: Option<&str>,
    peer: Option<IpAddr>,
    trusted: &[IpNet],
) -> Option<IpAddr> {
    match peer {
        // ① peer 存在且不受信 → 用 peer，忽略所有 header（防伪造）。
        Some(p) if !is_trusted(p, trusted) => Some(p),
        // ② peer 受信：走 header；header 全无 → 回落 peer（受信代理本身）。
        Some(p) => resolve_from_headers(xff, x_real_ip, trusted).or(Some(p)),
        // ③ peer None（UDS 无锚点）：保留既有 header 优先语义；全无 → None。
        None => resolve_from_headers(xff, x_real_ip, trusted),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }
    fn nets(items: &[&str]) -> TrustedNetworks {
        items.iter().map(|s| parse_network(s).unwrap()).collect()
    }
    fn peer(s: &str) -> Option<IpAddr> {
        Some(ip(s))
    }

    // ===== parse_network：CIDR + 裸 IP =====

    #[test]
    fn parse_network_accepts_cidr_and_bare_ip() {
        assert!(parse_network("10.0.0.0/8").unwrap().contains(&ip("10.1.2.3")));
        assert!(parse_network("192.168.1.1").unwrap().contains(&ip("192.168.1.1")));
        assert!(
            parse_network("::1").unwrap().contains(&ip("::1")),
            "bare v6 → /128"
        );
        assert!(parse_network("garbage").is_none());
        assert!(parse_network("").is_none());
    }

    #[test]
    fn parse_network_trims_whitespace() {
        assert!(parse_network("  10.0.0.0/8  ").is_some());
    }

    // ===== fail-safe 默认：trusted 空 =====

    #[test]
    fn empty_trusted_uses_peer_and_ignores_xff() {
        // 直连客户端伪造 XFF → 必须忽略，用 peer（修复前会信 XFF）。
        let r = extract_client_ip(Some("10.0.0.99"), None, peer("203.0.113.7"), &[]);
        assert_eq!(r, Some(ip("203.0.113.7")));
    }

    #[test]
    fn empty_trusted_uses_xff_when_no_peer() {
        // UDS 无 peer：保留既有 header 优先语义（无锚点，不引入新行为）。
        let r = extract_client_ip(Some("10.0.0.1"), None, None, &[]);
        assert_eq!(r, Some(ip("10.0.0.1")));
    }

    // ===== 评审 1.2：非受信 peer 携带 XFF → 忽略 XFF =====

    #[test]
    fn untrusted_peer_with_xff_ignores_xff() {
        let trusted = nets(&["10.0.0.0/8"]);
        // peer 203.0.113.7 不在受信段 → 用 peer，忽略 XFF（即使 XFF 看似合法）。
        let r = extract_client_ip(
            Some("10.0.0.5, 10.0.0.6"),
            None,
            peer("203.0.113.7"),
            &trusted,
        );
        assert_eq!(r, Some(ip("203.0.113.7")));
    }

    #[test]
    fn untrusted_peer_with_x_real_ip_ignores_real_ip() {
        let trusted = nets(&["10.0.0.0/8"]);
        let r = extract_client_ip(None, Some("10.0.0.9"), peer("203.0.113.7"), &trusted);
        assert_eq!(r, Some(ip("203.0.113.7")));
    }

    // ===== ② 受信 peer：右向左走 XFF =====

    #[test]
    fn single_trusted_proxy_yields_rightmost_untrusted() {
        // 客户端 → 受信代理(10.0.0.1) → us。XFF=[client]。
        let trusted = nets(&["10.0.0.0/8"]);
        let r = extract_client_ip(Some("203.0.113.55"), None, peer("10.0.0.1"), &trusted);
        assert_eq!(r, Some(ip("203.0.113.55")));
    }

    #[test]
    fn multi_hop_walks_right_to_left_to_first_untrusted() {
        // client → p1(受信) → p2(受信) → us。XFF=[client, p1, p2]。
        let trusted = nets(&["10.0.0.0/8"]);
        let xff = "203.0.113.55, 10.0.0.1, 10.0.0.2";
        let r = extract_client_ip(Some(xff), None, peer("10.0.0.2"), &trusted);
        assert_eq!(r, Some(ip("203.0.113.55")));
    }

    #[test]
    fn all_trusted_hops_yield_leftmost() {
        // 整条链全受信 → 取最左（best-guess 客户端）。
        let trusted = nets(&["10.0.0.0/8"]);
        let xff = "10.0.0.7, 10.0.0.8, 10.0.0.9";
        let r = extract_client_ip(Some(xff), None, peer("10.0.0.9"), &trusted);
        assert_eq!(r, Some(ip("10.0.0.7")));
    }

    // ===== 非法段终止（不跳过）=====

    #[test]
    fn invalid_segment_terminates_walk_not_skips() {
        // XFF=[真客户端, GARBAGE, 受信代理]。从右走：受信代理(continue)、
        // GARBAGE(非法→终止)。真客户端在非法段左侧 → 不可信 → fail-safe 回落。
        let trusted = nets(&["10.0.0.0/8"]);
        let xff = "203.0.113.55, GARBAGE, 10.0.0.1";
        let r = extract_client_ip(Some(xff), None, peer("10.0.0.1"), &trusted);
        // 终止于非法段，无合法非受信候选 → 回落受信 peer。
        assert_eq!(r, Some(ip("10.0.0.1")));
    }

    #[test]
    fn invalid_segment_before_untrusted_still_terminates() {
        // XFF=[受信代理, GARBAGE]。从右：GARBAGE→终止。无候选 → 回落 peer。
        let trusted = nets(&["10.0.0.0/8"]);
        let xff = "10.0.0.1, not-an-ip";
        let r = extract_client_ip(Some(xff), None, peer("10.0.0.1"), &trusted);
        assert_eq!(r, Some(ip("10.0.0.1")));
    }

    // ===== ④ 无 XFF → X-Real-IP / peer =====

    #[test]
    fn trusted_peer_no_xff_uses_real_ip() {
        let trusted = nets(&["10.0.0.0/8"]);
        let r = extract_client_ip(None, Some("203.0.113.55"), peer("10.0.0.1"), &trusted);
        assert_eq!(r, Some(ip("203.0.113.55")));
    }

    #[test]
    fn trusted_peer_no_headers_falls_back_to_peer() {
        let trusted = nets(&["10.0.0.0/8"]);
        let r = extract_client_ip(None, None, peer("10.0.0.1"), &trusted);
        assert_eq!(r, Some(ip("10.0.0.1")));
    }

    #[test]
    fn no_peer_no_headers_returns_none() {
        assert_eq!(extract_client_ip(None, None, None, &[]), None);
    }

    // ===== 重复 XFF 头合并（RFC 出现序）=====

    #[test]
    fn merge_xff_preserves_rfc_order() {
        // 两个 XFF 头按出现序合并后右向左走，取首个非受信。
        let merged = merge_xff(["203.0.113.55, 10.0.0.1", "10.0.0.2"]).unwrap();
        let trusted = nets(&["10.0.0.0/8"]);
        let r = extract_client_ip(Some(&merged), None, peer("10.0.0.2"), &trusted);
        assert_eq!(r, Some(ip("203.0.113.55")));
    }

    #[test]
    fn merge_xff_empty_when_all_blank() {
        assert_eq!(merge_xff(["", "  "]), None);
        assert_eq!(merge_xff(std::iter::empty::<&str>()), None);
    }

    // ===== v4/v6 混合 =====

    #[test]
    fn mixed_v4_v6_segments() {
        // 受信代理含 v6；客户端 v4。
        let trusted = nets(&["2001:db8::/32"]);
        let xff = "203.0.113.55, 2001:db8::1";
        let r = extract_client_ip(Some(xff), None, peer("2001:db8::1"), &trusted);
        assert_eq!(r, Some(ip("203.0.113.55")));
    }

    #[test]
    fn v6_client_returned_verbatim() {
        let trusted = nets(&["10.0.0.0/8"]);
        let r = extract_client_ip(Some("2001:db8::42"), None, peer("10.0.0.1"), &trusted);
        assert_eq!(r, Some(ip("2001:db8::42")));
    }

    // ===== property-style：受信 peer + 单段 XFF = 该段 =====

    #[test]
    fn fuzz_single_segment_xff_under_trusted_proxy() {
        // 任意合法单段 XFF，在受信 peer 下都应原样返回（清洗不篡改合法输入）。
        let trusted = nets(&["10.0.0.0/8"]);
        for raw in [
            "1.2.3.4",
            "0.0.0.0",
            "255.255.255.255",
            "::1",
            "fe80::1",
            "198.51.100.7",
        ] {
            let parsed: IpAddr = raw.parse().unwrap();
            let r = extract_client_ip(Some(raw), None, peer("10.0.0.1"), &trusted);
            assert_eq!(r, Some(parsed), "single XFF segment {raw} should pass through");
        }
    }

    #[test]
    fn fuzz_untrusted_peer_always_ignores_headers() {
        // 任意 header 组合，非受信 peer → 一律用 peer（核心 fail-safe 不变式）。
        let trusted = nets(&["10.0.0.0/8"]);
        for (xff, xri) in [
            (Some("10.0.0.5"), None),
            (None, Some("10.0.0.6")),
            (Some("10.0.0.5, 10.0.0.6"), Some("10.0.0.7")),
            (Some("not-an-ip"), None),
        ] {
            let r = extract_client_ip(xff, xri, peer("203.0.113.7"), &trusted);
            assert_eq!(
                r,
                Some(ip("203.0.113.7")),
                "untrusted peer must win over any header (xff={xff:?})"
            );
        }
    }

    // ===== loopback 判定不靠 XFF（与 P7-1 协同）=====

    #[test]
    fn loopback_peer_untrusted_when_not_configured() {
        // 127.0.0.1 不在 trusted → 用 127.0.0.1（即 peer），XFF 被忽略。
        // P7-1 的 loopback 判定也用 peer，不用 XFF——一致。
        let r = extract_client_ip(Some("10.0.0.99"), None, peer("127.0.0.1"), &[]);
        assert_eq!(r, Some(ip("127.0.0.1")));
    }
}

/// §6.7 解析面 property 测试（proptest）：XFF/CIDR 清洗的安全不变式——
/// 非受信 peer 恒胜、结果无捏造、非法段终止右移（不跳过）。
#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    /// 任意 v4/v6 地址。
    fn arb_ip() -> impl Strategy<Value = IpAddr> {
        prop_oneof![
            any::<u32>().prop_map(|b| IpAddr::from(Ipv4Addr::from(b))),
            any::<u128>().prop_map(|b| IpAddr::from(Ipv6Addr::from(b))),
        ]
    }

    /// 任意合法 CIDR。
    fn arb_net() -> impl Strategy<Value = IpNet> {
        prop_oneof![
            (any::<u32>(), 0u8..=32)
                .prop_map(|(a, p)| IpNet::V4(ipnet::Ipv4Net::new(Ipv4Addr::from(a), p).unwrap())),
            (any::<u128>(), 0u8..=128)
                .prop_map(|(a, p)| IpNet::V6(ipnet::Ipv6Net::new(Ipv6Addr::from(a), p).unwrap())),
        ]
    }

    /// XFF 段：合法 IP / v4-ish / v6-ish / 纯垃圾混合，逼近真实畸形输入。
    fn arb_segment() -> impl Strategy<Value = String> {
        prop_oneof![
            3 => arb_ip().prop_map(|ip| ip.to_string()),
            1 => "[0-9.]{1,10}",
            1 => "[0-9a-fA-F:]{1,16}",
            1 => "[a-zA-Z]{1,8}",
        ]
    }

    /// 受信 peer 场景：50% 概率把一个由 peer 派生的网段塞进 trusted，
    /// 保证假设 `is_trusted(p)` 有足量通过样本。
    fn arb_trusted_scenario()
    -> impl Strategy<Value = (Vec<String>, Option<IpAddr>, IpAddr, Vec<IpNet>)> {
        (
            prop::collection::vec(arb_segment(), 0..8),
            prop::option::of(arb_ip()),
            arb_ip(),
            prop::collection::vec(arb_net(), 0..2),
            any::<bool>(),
            0u8..=128,
        )
            .prop_map(|(segs, xri, p, mut nets, include_peer, prefix)| {
                if include_peer {
                    let max = if p.is_ipv4() { 32 } else { 128 };
                    nets.push(IpNet::new(p, prefix.min(max)).unwrap());
                }
                (segs, xri, p, nets)
            })
    }

    proptest! {
        /// 核心 fail-safe 不变式：非受信 peer 一律胜出，header 再花哨也忽略。
        #[test]
        fn untrusted_peer_always_wins(
            xff in prop::option::of(prop::collection::vec(arb_segment(), 0..6)
                .prop_map(|v| v.join(", "))),
            xri in prop::option::of("[a-zA-Z0-9.:]{1,20}"),
            p in arb_ip(),
            trusted in prop::collection::vec(arb_net(), 0..3),
        ) {
            prop_assume!(!is_trusted(p, &trusted));
            let r = extract_client_ip(xff.as_deref(), xri.as_deref(), Some(p), &trusted);
            prop_assert_eq!(r, Some(p));
        }

        /// 无捏造：返回 IP 必来自 peer / X-Real-IP / 某个合法 XFF 段。
        /// 且若来自 XFF 段，其下标必在"最右非法段"右侧（非法段终止右移，不跳过）。
        #[test]
        fn result_never_fabricated_and_respects_termination(
            (segs, xri, p, trusted) in arb_trusted_scenario(),
        ) {
            prop_assume!(is_trusted(p, &trusted)); // 走 header 路径
            let xff = segs.join(", ");
            let xri_s = xri.map(|i| i.to_string());
            let r = extract_client_ip(Some(&xff), xri_s.as_deref(), Some(p), &trusted);
            let Some(r) = r else { return Ok(()); };
            if r == p || Some(r) == xri {
                return Ok(());
            }
            // 与 walk_xff 同口径清洗段序列（split/trim/滤空）后比对
            let cleaned: Vec<&str> =
                xff.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
            prop_assert!(
                cleaned.iter().any(|s| s.parse::<IpAddr>() == Ok(r)),
                "result {} not from any XFF segment of {:?}", r, xff
            );
            let rightmost_invalid =
                cleaned.iter().rposition(|s| s.parse::<IpAddr>().is_err());
            if let Some(ri) = rightmost_invalid {
                prop_assert!(
                    cleaned.iter().enumerate()
                        .any(|(i, s)| i > ri && s.parse::<IpAddr>() == Ok(r)),
                    "segment left of invalid segment {} was trusted (xff={:?})", ri, xff
                );
            }
        }

        /// parse_network 与 std 解析严格等价：Some ⟺ trim 后按 IpNet 或 IpAddr 可解析；
        /// 裸 IP 条目必含其自身（/32、/128 主机路由）。
        #[test]
        fn parse_network_iff_std_parses(s in ".*") {
            let parsed = parse_network(&s);
            let std_ok =
                s.trim().parse::<IpNet>().is_ok() || s.trim().parse::<IpAddr>().is_ok();
            prop_assert_eq!(parsed.is_some(), std_ok, "input: {:?}", s);
            if let (Some(net), Ok(ip)) = (parsed, s.trim().parse::<IpAddr>()) {
                prop_assert!(net.contains(&ip));
            }
        }

        /// merge_xff：None ⟺ 全空白；Some 时 = 各非空段 trim 后按 ", " 连接（保序）。
        #[test]
        fn merge_xff_preserves_segments(
            vals in prop::collection::vec("[a-zA-Z0-9.: ]{0,12}", 0..6),
        ) {
            let merged = merge_xff(&vals);
            let nonblank: Vec<&str> =
                vals.iter().map(|v| v.trim()).filter(|s| !s.is_empty()).collect();
            if nonblank.is_empty() {
                prop_assert!(merged.is_none());
            } else {
                let m = merged.unwrap();
                let parts: Vec<&str> = m.split(", ").collect();
                prop_assert_eq!(parts, nonblank);
            }
        }
    }
}
