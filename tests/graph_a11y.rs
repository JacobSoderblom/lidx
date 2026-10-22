//! Legibility pins for the `lidx graph` viewer's stylesheet: WCAG AA contrast
//! of every text token on every paper in both themes (computed from the
//! OKLCH values the page ships), the 11px floor, 24px tap targets, pressed
//! controls that survive hover, and one reduced-motion rule.

use std::collections::HashMap;

const CSS: &str = include_str!("../src/graph_html/viewer.css");
const JS: &str = include_str!("../src/graph_html/viewer.js");
const TEMPLATE: &str = include_str!("../src/graph_html/template.html");

/// `--name: value;` pairs of the block that starts at `head`.
fn block(head: &str) -> HashMap<String, String> {
    let start = CSS.find(head).unwrap_or_else(|| panic!("no block {head}")) + head.len();
    let end = start + CSS[start..].find('}').unwrap();
    let mut out = HashMap::new();
    for decl in CSS[start..end].split(';') {
        // Drop comments, then read `--name: value`.
        let mut d = decl.to_string();
        while let (Some(a), Some(b)) = (d.find("/*"), d.find("*/")) {
            d.replace_range(a..b + 2, "");
        }
        let d = d.trim();
        if let Some((k, v)) = d.strip_prefix("--").and_then(|r| r.split_once(':')) {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    out
}

fn light() -> HashMap<String, String> {
    block(":root {\n  color-scheme: light;")
}
fn dark_media() -> HashMap<String, String> {
    block("@media (prefers-color-scheme: dark) {\n  :root:not([data-theme=\"light\"]) {")
}
fn dark_attr() -> HashMap<String, String> {
    block(":root[data-theme=\"dark\"] {")
}

/// Linear sRGB of `oklch(L% C H)` or `#rrggbb`.
fn linear(v: &str) -> [f64; 3] {
    if let Some(hex) = v.strip_prefix('#') {
        let n = u32::from_str_radix(hex, 16).unwrap();
        return [(n >> 16) & 255, (n >> 8) & 255, n & 255].map(|c| {
            let c = c as f64 / 255.0;
            if c <= 0.04045 {
                c / 12.92
            } else {
                ((c + 0.055) / 1.055).powf(2.4)
            }
        });
    }
    let inner = v
        .strip_prefix("oklch(")
        .unwrap_or_else(|| panic!("{v}"))
        .trim_end_matches(')');
    let p: Vec<f64> = inner
        .split_whitespace()
        .map(|x| x.trim_end_matches('%').parse().unwrap())
        .collect();
    let (l, c, h) = (p[0] / 100.0, p[1], p[2].to_radians());
    let (a, b) = (c * h.cos(), c * h.sin());
    let l_ = (l + 0.3963377774 * a + 0.2158037573 * b).powi(3);
    let m_ = (l - 0.1055613458 * a - 0.0638541728 * b).powi(3);
    let s_ = (l - 0.0894841775 * a - 1.291485548 * b).powi(3);
    [
        4.0767416621 * l_ - 3.3077115913 * m_ + 0.2309699292 * s_,
        -1.2684380046 * l_ + 2.6097574011 * m_ - 0.3413193965 * s_,
        -0.0041960863 * l_ - 0.7034186147 * m_ + 1.707614701 * s_,
    ]
    .map(|x| x.clamp(0.0, 1.0))
}

fn luminance(c: [f64; 3]) -> f64 {
    0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]
}

fn contrast(fg: &str, bg: &str) -> f64 {
    let (a, b) = (luminance(linear(fg)), luminance(linear(bg)));
    (a.max(b) + 0.05) / (a.min(b) + 0.05)
}

/// Text tokens and the surfaces they sit on, per theme.
fn pairs(theme: &HashMap<String, String>, dark: bool) -> Vec<(String, String, f64)> {
    let g = |k: &str| theme.get(k).unwrap_or_else(|| panic!("--{k}")).clone();
    let papers = ["paper", "paper-2", "paper-3", "canvas"];
    let mut out = Vec::new();
    let mut add = |fg: &str, bgs: &[&str]| {
        for bg in bgs {
            out.push((fg.to_string(), bg.to_string(), contrast(&g(fg), &g(bg))));
        }
    };
    add("ink", &papers);
    add("ink-2", &papers);
    add("ink-3", &papers);
    // The redline and the bridge colour as text, and as the fill under paper text.
    add("focus-ink", &papers);
    add("bridge-ink", &papers);
    // Resource kind labels sit on the tray (paper-2) or, for outside systems, paper.
    for k in [
        "r-db", "r-cache", "r-search", "r-store", "r-secret", "r-ext",
    ] {
        add(k, &["paper", "paper-2"]);
    }
    // Paper-coloured text on a pressed or hovered fill.
    for (fg, bg) in [("paper", "ink"), ("paper", "focus-ink")] {
        out.push((fg.to_string(), bg.to_string(), contrast(&g(fg), &g(bg))));
    }
    let _ = dark;
    out
}

#[test]
fn every_text_token_passes_aa_on_every_paper_in_both_themes() {
    for (name, theme, dark) in [
        ("light", light(), false),
        ("dark media", dark_media(), true),
        ("dark attr", dark_attr(), true),
    ] {
        // Dark blocks only redefine some tokens: fill in the rest from light.
        let mut full = light();
        full.extend(theme);
        for (fg, bg, ratio) in pairs(&full, dark) {
            assert!(ratio >= 4.5, "{name}: --{fg} on --{bg} is {ratio:.2}:1");
        }
    }
}

#[test]
fn text_tokens_are_pinned() {
    let l = light();
    assert_eq!(l["ink-2"], "oklch(40% 0.022 255)");
    assert_eq!(l["ink-3"], "oklch(49% 0.018 255)");
    assert_eq!(l["focus-ink"], "oklch(50% 0.16 45)");
    assert_eq!(l["bridge-ink"], "oklch(50% 0.13 345)");
    assert_eq!(l["r-ext"], "oklch(49% 0.018 255)");
    // The redline itself stays Survey Orange: only its text variant is darker.
    assert_eq!(l["focus"], "#D55E00");
    for d in [dark_media(), dark_attr()] {
        assert_eq!(d["focus"], "#F07A2E");
        assert_eq!(d["focus-ink"], "#F07A2E");
        assert_eq!(d["bridge-ink"], "#D98CBD");
        assert_eq!(d["ink-3"], "oklch(66% 0.015 255)");
    }
    // Measured values the audit quoted: the old tokens failed, the new ones pass.
    assert!(contrast("#D55E00", &l["paper"]) < 4.5);
    assert!((contrast(&l["ink-3"], &l["paper-3"]) - 4.79).abs() < 0.02);
    assert!((contrast(&l["focus-ink"], &l["paper-3"]) - 4.90).abs() < 0.02);
}

#[test]
fn no_text_is_smaller_than_11px() {
    let mut small = Vec::new();
    for (n, line) in CSS.lines().enumerate() {
        for decl in line.split(';') {
            let d = decl.trim();
            let size = match (d.find("font-size:"), d.find("font:")) {
                (Some(i), _) => d[i + 10..].trim().split('p').next().map(str::to_string),
                (None, Some(i)) => d[i + 5..]
                    .split_whitespace()
                    .find(|t| t.ends_with("px"))
                    .map(|t| t.trim_end_matches("px").to_string()),
                _ => None,
            };
            let below = size
                .and_then(|s| s.trim().parse::<f64>().ok())
                .is_some_and(|v| v < 11.0);
            if below {
                small.push(format!("viewer.css:{}: {d}", n + 1));
            }
        }
    }
    // Canvas and SVG text: any font declaration with a px size under 11.
    for (n, line) in JS.lines().enumerate() {
        let declares =
            line.contains(".font =") || line.contains("font-size") || line.contains("font:");
        let below = line
            .split(['\'', '"', ' '])
            .filter_map(|t| t.strip_suffix("px"))
            .filter_map(|v| v.parse::<f64>().ok())
            .any(|v| v > 0.0 && v < 11.0);
        if declares && below {
            small.push(format!("viewer.js:{}: {}", n + 1, line.trim()));
        }
    }
    assert!(small.is_empty(), "{small:#?}");
}

/// The heading steps the page-level detector wants: h1 well above body.
#[test]
fn headings_step_clearly() {
    assert!(CSS.contains("h1 { margin: 0; font-size: 17px;"));
    assert!(CSS.contains("dialog h2 { margin: 0 0 10px; font-size: 18px; }"));
}

#[test]
fn pressed_controls_stay_inverted_under_hover() {
    // Hover never restyles a pressed button, so ink on ink cannot happen.
    assert!(CSS.contains(".tools button:hover:not(:disabled):not([aria-pressed=\"true\"])"));
    assert!(CSS.contains(".panel .modes button:hover:not([aria-pressed=\"true\"])"));
    assert!(CSS.contains(".g-tours button:hover:not(.pri):not([aria-pressed=\"true\"])"));
    assert!(CSS.contains(
        ".tools button[aria-pressed=\"true\"] { background: var(--ink); color: var(--paper);"
    ));
    assert!(
        CSS.contains(".g-follow:hover, .g-follow:focus-visible { background: var(--focus-ink);")
    );
}

#[test]
fn small_targets_are_24px_tall() {
    for sel in [
        ".g-loc { display: inline-flex; align-items: center; min-height: 24px;",
        ".g-nm { display: inline-flex; align-items: center; min-height: 24px;",
        ".g-link { display: inline-flex; align-items: center; min-height: 24px;",
        ".g-backlink { display: inline-flex; align-items: center; min-height: 24px;",
        "min-height: 24px; margin-top: -4px; margin-bottom: -4px;",
        ".g-tours .g-dot { position: relative; width: 24px; height: 24px;",
        ".panel .vt-row .tw { flex: none; width: 24px; height: 24px;",
        ".crumbs button {\n  border: 0; background: none; padding: 2px 5px; min-height: 24px;",
    ] {
        assert!(CSS.contains(sel), "{sel}");
    }
}

#[test]
fn one_rule_removes_every_transition_under_reduced_motion() {
    assert_eq!(CSS.matches("prefers-reduced-motion: reduce").count(), 1);
    assert!(CSS.contains("*, *::before, *::after { transition: none !important; animation: none !important; scroll-behavior: auto !important; }"));
}

#[test]
fn labels_name_what_they_do() {
    assert!(TEMPLATE.contains(
        "id=\"search-label\" class=\"sr-only\">Search apps, symbols, files and folders</label>"
    ));
    assert!(!TEMPLATE.contains("Search nodes"));
    // A live region on the depth label would announce "depth 1".
    assert!(TEMPLATE.contains("<span id=\"depth-label\" class=\"depth-label\">folders</span>"));
    assert!(
        !TEMPLATE.contains("id=\"g-side\" class=\"g-side\" aria-label=\"Start here\" aria-live")
    );
    // The two "on focus" legend buttons are told apart by their family.
    assert!(JS.contains("APP_FAM[f].label + ' links, '"));
}
