//! A graph's node style: icon, colour and caption per label, colour per
//! relationship type (`samyama.icons/1`, #2029).
//!
//! The style travels in the `.sgsnap` header, so a snapshot cannot be shared
//! without it, and every client draws a label the same way. It is small (a few
//! KB) and capped at [`MAX_STYLE_BYTES`] and [`MAX_ENTRIES`].
//!
//! Reading a style is lenient: a bad entry is skipped with a warning and never
//! fails an import, because a wrong colour is no reason to refuse a graph.
//! [`IconStyle::strict`] is the form for a client replacing the style, which
//! should hear about every mistake instead.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The only format this build reads.
pub const FORMAT: &str = "samyama.icons/1";

/// Largest style accepted, as JSON.
pub const MAX_STYLE_BYTES: usize = 64 * 1024;

/// Most label and relationship-type entries a style may hold, together.
pub const MAX_ENTRIES: usize = 1024;

const MAX_KEY_CHARS: usize = 256;
const MAX_ICON_NAME_CHARS: usize = 64;
const MAX_CAPTION_CHARS: usize = 128;

/// How one node label is drawn. Every field is optional; a field left out uses
/// the client's own rule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStyle {
    /// `#RRGGBB`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// An icon name, `lucide:<Name>`; never image data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// The property shown as the node's text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
}

/// How one relationship type is drawn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeStyle {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

/// A graph's style. Deserializing never fails: what cannot be read is left out
/// and described in [`IconStyle::warnings`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Value")]
pub struct IconStyle {
    pub format: String,
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeStyle>,
    #[serde(default)]
    pub edges: BTreeMap<String, EdgeStyle>,
    /// What was skipped while reading, one line each. Not written out.
    #[serde(skip)]
    pub warnings: Vec<String>,
}

impl From<Value> for IconStyle {
    fn from(v: Value) -> Self {
        IconStyle::read(&v)
    }
}

/// What a style covers, against a schema. The `icons` object of a snapshot
/// import response and of `PUT /api/icons`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct IconReport {
    /// `header` (loaded from the snapshot), `api` (set by `PUT /api/icons`) or
    /// `none`.
    pub source: &'static str,
    /// Schema labels the style has an entry for.
    pub styled: usize,
    /// Schema labels it has none for; clients draw these by their own rules.
    pub unstyled: Vec<String>,
    /// Labels it styles that the schema does not have.
    pub unknown_labels: Vec<String>,
    /// Relationship types it colours that the schema does not have.
    pub unknown_edge_types: Vec<String>,
    /// Entries or fields that were skipped, and why.
    pub warnings: Vec<String>,
}

impl IconStyle {
    /// An empty style in the current format.
    pub fn empty() -> Self {
        IconStyle {
            format: FORMAT.to_string(),
            ..Default::default()
        }
    }

    /// Whether the style says anything.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }

    /// Reads a style, skipping what is invalid with a warning.
    ///
    /// An unknown `format`, or a style over [`MAX_STYLE_BYTES`], is skipped
    /// whole: there is no telling which of its entries mean what this build
    /// would take them to mean. Otherwise an invalid field is dropped, and an
    /// entry left with no field is dropped.
    pub fn read(v: &Value) -> Self {
        let mut out = IconStyle::default();
        let Some(obj) = v.as_object() else {
            out.warnings
                .push("the style is not a JSON object; it was ignored".to_string());
            return out;
        };
        out.format = obj
            .get("format")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if out.format != FORMAT {
            out.warnings.push(format!(
                "unknown style format {:?} (this build reads {FORMAT:?}); the style was ignored",
                out.format
            ));
            return out;
        }
        let size = serde_json::to_vec(v).map(|b| b.len()).unwrap_or(usize::MAX);
        if size > MAX_STYLE_BYTES {
            out.warnings.push(format!(
                "the style is {size} bytes, over the {MAX_STYLE_BYTES}-byte limit; it was ignored"
            ));
            return out;
        }
        for key in obj.keys() {
            if !matches!(key.as_str(), "format" | "nodes" | "edges") {
                out.warnings
                    .push(format!("unknown style field {key:?} was ignored"));
            }
        }

        let mut room = MAX_ENTRIES;
        for (kind, field) in [("label", "nodes"), ("relationship type", "edges")] {
            let entries = match obj.get(field) {
                None | Some(Value::Null) => continue,
                Some(Value::Object(m)) => m,
                Some(_) => {
                    out.warnings
                        .push(format!("`{field}` is not an object; it was ignored"));
                    continue;
                }
            };
            for (name, entry) in entries {
                if room == 0 {
                    out.warnings.push(format!(
                        "more than {MAX_ENTRIES} entries; {kind} {name:?} and those after it were ignored"
                    ));
                    break;
                }
                let at = format!("{kind} {name:?}");
                if name.trim().is_empty() || name.chars().count() > MAX_KEY_CHARS {
                    out.warnings.push(format!(
                        "{at}: a name must be 1 to {MAX_KEY_CHARS} characters; ignored"
                    ));
                    continue;
                }
                let Some(entry) = entry.as_object() else {
                    out.warnings
                        .push(format!("{at}: the entry is not an object; ignored"));
                    continue;
                };
                let allowed: &[&str] = if field == "nodes" {
                    &["color", "icon", "caption"]
                } else {
                    &["color"]
                };
                for key in entry.keys() {
                    if !allowed.contains(&key.as_str()) {
                        out.warnings
                            .push(format!("{at}: unknown field {key:?} was ignored"));
                    }
                }
                let color = field_of(entry, "color", &at, &mut out.warnings, valid_color);
                if field == "nodes" {
                    let style = NodeStyle {
                        color,
                        icon: field_of(entry, "icon", &at, &mut out.warnings, valid_icon),
                        caption: field_of(entry, "caption", &at, &mut out.warnings, valid_caption),
                    };
                    if style == NodeStyle::default() {
                        out.warnings
                            .push(format!("{at}: no valid field left; the entry was skipped"));
                        continue;
                    }
                    out.nodes.insert(name.clone(), style);
                } else {
                    if color.is_none() {
                        out.warnings
                            .push(format!("{at}: no valid field left; the entry was skipped"));
                        continue;
                    }
                    out.edges.insert(name.clone(), EdgeStyle { color });
                }
                room -= 1;
            }
        }
        out
    }

    /// Reads a style and refuses it if anything in it would be skipped. A
    /// missing `format` is taken as the current one.
    pub fn strict(v: &Value) -> Result<Self, Vec<String>> {
        let mut v = v.clone();
        if let Some(obj) = v.as_object_mut() {
            obj.entry("format")
                .or_insert_with(|| Value::String(FORMAT.to_string()));
        }
        let style = IconStyle::read(&v);
        if style.warnings.is_empty() {
            Ok(style)
        } else {
            Err(style.warnings)
        }
    }

    /// A starting style for a schema: today's default colour and icon for each
    /// label, as Samyama Insight assigns them. Labels `existing` already styles
    /// keep their entry, so a label added later never moves the others.
    pub fn initial(labels: &[String], existing: Option<&IconStyle>) -> Self {
        let mut style = existing
            .filter(|s| s.format == FORMAT)
            .cloned()
            .unwrap_or_else(IconStyle::empty);
        style.warnings.clear();
        let colours = default_label_colors(labels);
        for label in labels {
            style
                .nodes
                .entry(label.clone())
                .or_insert_with(|| NodeStyle {
                    color: colours.get(label.as_str()).map(|c| c.to_string()),
                    icon: default_icon(label).map(|n| format!("lucide:{n}")),
                    caption: None,
                });
        }
        style
    }

    /// What this style covers of a schema, without changing it.
    pub fn report(
        &self,
        source: &'static str,
        labels: &[String],
        edge_types: &[String],
    ) -> IconReport {
        IconReport {
            source,
            styled: labels
                .iter()
                .filter(|l| self.nodes.contains_key(*l))
                .count(),
            unstyled: labels
                .iter()
                .filter(|l| !self.nodes.contains_key(*l))
                .cloned()
                .collect(),
            unknown_labels: self
                .nodes
                .keys()
                .filter(|l| !labels.contains(l))
                .cloned()
                .collect(),
            unknown_edge_types: self
                .edges
                .keys()
                .filter(|t| !edge_types.contains(t))
                .cloned()
                .collect(),
            warnings: self.warnings.clone(),
        }
    }

    /// The style an import loads, and its report. Entries for labels and
    /// relationship types the schema lacks are listed and left out; a header
    /// with no style, or one that was skipped whole, loads none.
    pub fn for_import(
        header: Option<&IconStyle>,
        labels: &[String],
        edge_types: &[String],
    ) -> (Option<IconStyle>, IconReport) {
        let Some(style) = header else {
            return (None, IconStyle::empty().report("none", labels, edge_types));
        };
        if style.format != FORMAT {
            let mut report = IconStyle::empty().report("none", labels, edge_types);
            report.warnings = style.warnings.clone();
            return (None, report);
        }
        let report = style.report("header", labels, edge_types);
        let mut kept = style.clone();
        kept.nodes.retain(|l, _| labels.contains(l));
        kept.edges.retain(|t, _| edge_types.contains(t));
        kept.warnings.clear();
        (Some(kept), report)
    }
}

fn field_of(
    entry: &serde_json::Map<String, Value>,
    key: &str,
    at: &str,
    warnings: &mut Vec<String>,
    check: fn(&str) -> Result<(), String>,
) -> Option<String> {
    match entry.get(key)? {
        Value::Null => None,
        Value::String(s) => match check(s) {
            Ok(()) => Some(s.clone()),
            Err(why) => {
                warnings.push(format!("{at}: {key} {s:?} {why}; skipped"));
                None
            }
        },
        other => {
            warnings.push(format!(
                "{at}: {key} must be a string, got {other}; skipped"
            ));
            None
        }
    }
}

fn valid_color(s: &str) -> Result<(), String> {
    let hex = s.strip_prefix('#').unwrap_or_default();
    if s.len() == 7 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("is not a #RRGGBB colour".to_string())
    }
}

fn valid_icon(s: &str) -> Result<(), String> {
    match s.strip_prefix("lucide:") {
        Some(n)
            if !n.is_empty()
                && n.len() <= MAX_ICON_NAME_CHARS
                && n.chars().all(|c| c.is_ascii_alphanumeric()) =>
        {
            Ok(())
        }
        _ => Err("is not an icon name of the form lucide:<Name>".to_string()),
    }
}

fn valid_caption(s: &str) -> Result<(), String> {
    if s.trim().is_empty()
        || s.chars().count() > MAX_CAPTION_CHARS
        || s.chars().any(char::is_control)
    {
        Err(format!(
            "is not a property name (1 to {MAX_CAPTION_CHARS} characters, no control characters)"
        ))
    } else {
        Ok(())
    }
}

/// Samyama Insight's label palette (`src/lib/palette.ts`, `LABEL_PALETTE`).
pub const LABEL_PALETTE: [&str; 10] = [
    "#60a5fa", "#34d399", "#fbbf24", "#f87171", "#a78bfa", "#f472b6", "#22d3ee", "#fb923c",
    "#2dd4bf", "#818cf8",
];

/// Insight's `hashSlot`: a 32-bit string hash over UTF-16 code units.
fn hash_slot(label: &str, size: usize) -> usize {
    let mut h: i32 = 0;
    for u in label.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(i32::from(u));
    }
    (i64::from(h).unsigned_abs() % size as u64) as usize
}

/// Insight's `assignLabelColors`: sort the labels, give each the slot its
/// name hashes to, and probe to the next free slot on a collision.
pub fn default_label_colors(labels: &[String]) -> BTreeMap<&str, &'static str> {
    let mut sorted: Vec<&str> = labels.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();
    let size = LABEL_PALETTE.len();
    let mut taken = [false; LABEL_PALETTE.len()];
    let mut out = BTreeMap::new();
    for label in sorted {
        let mut idx = hash_slot(label, size);
        let mut guard = 0;
        while taken[idx] && guard < size {
            idx = (idx + 1) % size;
            guard += 1;
        }
        taken[idx] = true;
        out.insert(label, LABEL_PALETTE[idx]);
    }
    out
}

fn normalise(label: &str) -> String {
    label
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        .collect()
}

/// Insight's default icon for a label (`nodeIcons.ts`): the exact name first,
/// then the longest word the name contains. `None` where Insight draws its
/// plain circle.
pub fn default_icon(label: &str) -> Option<&'static str> {
    let n = normalise(label);
    if let Some((_, icon)) = ICON_BY_LABEL.iter().find(|(k, _)| *k == n) {
        return Some(icon);
    }
    ICON_BY_KEYWORD
        .iter()
        .find(|(word, _)| n.contains(word))
        .map(|(_, icon)| *icon)
}

// Generated from samyama-insight `src/lib/nodeIcons.ts`: `BY_LABEL`, and `BY_KEYWORD`
// in the order Insight tries it (longest word first).
pub(crate) const ICON_BY_LABEL: &[(&str, &str)] = &[
    ("account", "WalletCards"),
    ("accountholder", "Contact"),
    ("accountkyc", "IdCard"),
    ("accountstatusevent", "Milestone"),
    ("address", "MapPin"),
    ("admissibilityflag", "Flag"),
    ("adverseevent", "Activity"),
    ("agency", "Building2"),
    ("aggregate", "Sigma"),
    ("agreement", "Signature"),
    ("aircraft", "Plane"),
    ("airport", "Plane"),
    ("alarm", "BellRing"),
    ("alert", "Siren"),
    ("alertevent", "BellRing"),
    ("amount", "IndianRupee"),
    ("anomalyevent", "AlertTriangle"),
    ("api", "Webhook"),
    ("application", "ClipboardCheck"),
    ("appointment", "CalendarClock"),
    ("arrest", "UserX"),
    ("article", "Newspaper"),
    ("asset", "Package"),
    ("assetcategory", "Package"),
    ("assettag", "Tag"),
    ("assignment", "ClipboardList"),
    ("attachment", "Paperclip"),
    ("audio", "Music"),
    ("author", "User"),
    ("award", "Award"),
    ("bank", "Landmark"),
    ("bankaccount", "WalletCards"),
    ("barcode", "Barcode"),
    ("batch", "Layers"),
    ("battery", "Battery"),
    ("beneficiary", "UserCheck"),
    ("bicycle", "Bike"),
    ("bill", "Receipt"),
    ("bin", "Boxes"),
    ("bird", "Bird"),
    ("blower", "Wind"),
    ("boiler", "Flame"),
    ("bone", "Bone"),
    ("brain", "Brain"),
    ("branch", "Building"),
    ("breaker", "PowerOff"),
    ("budget", "PiggyBank"),
    ("bus", "Bus"),
    ("cache", "Zap"),
    ("calculation", "Calculator"),
    ("calibration", "Ruler"),
    ("campaign", "Megaphone"),
    ("car", "Car"),
    ("card", "CreditCard"),
    ("case", "FolderSearch"),
    ("casestatusevent", "Milestone"),
    ("cash", "Banknote"),
    ("category", "Layers"),
    ("certificate", "Stamp"),
    ("channel", "Radio"),
    ("chargeback", "Undo2"),
    ("chargesheet", "ScrollText"),
    ("child", "Baby"),
    ("chiller", "Snowflake"),
    ("church", "Church"),
    ("citizen", "User"),
    ("city", "Building"),
    ("clinic", "Stethoscope"),
    ("cluster", "Boxes"),
    ("cohort", "Users"),
    ("college", "School"),
    ("commit", "GitCommitHorizontal"),
    ("company", "Building2"),
    ("complaint", "FileText"),
    ("complaintnote", "StickyNote"),
    ("complaintstatusevent", "Milestone"),
    ("component", "Component"),
    ("compound", "FlaskConical"),
    ("compressor", "Wind"),
    ("condition", "Stethoscope"),
    ("conduit", "Waypoints"),
    ("config", "Settings"),
    ("contact", "Contact"),
    ("container", "Container"),
    ("contract", "Signature"),
    ("control", "ShieldCheck"),
    ("cooler", "Snowflake"),
    ("country", "Globe"),
    ("course", "Book"),
    ("court", "Gavel"),
    ("credential", "KeyRound"),
    ("creditcard", "CreditCard"),
    ("crew", "Users"),
    ("crime", "ShieldAlert"),
    ("currency", "Coins"),
    ("curriculum", "Scroll"),
    ("custodyevent", "Lock"),
    ("customer", "UserRound"),
    ("cyberdocket", "FolderOpen"),
    ("database", "Database"),
    ("delivery", "PackageCheck"),
    ("deployment", "Boxes"),
    ("deposit", "PiggyBank"),
    ("design", "Palette"),
    ("device", "MonitorSmartphone"),
    ("diagnosis", "Stethoscope"),
    ("disease", "HeartPulse"),
    ("diseasearea", "HeartPulse"),
    ("diseasecategory", "HeartPulse"),
    ("diseasegroup", "HeartPulse"),
    ("diseasetype", "HeartPulse"),
    ("disk", "HardDrive"),
    ("distribution", "PieChart"),
    ("district", "Map"),
    ("docketentry", "ListOrdered"),
    ("doctor", "Stethoscope"),
    ("document", "File"),
    ("domain", "Globe"),
    ("dose", "Syringe"),
    ("downtime", "PowerOff"),
    ("drug", "Pill"),
    ("drugclass", "Pill"),
    ("duct", "Waypoints"),
    ("duration", "Timer"),
    ("edge", "Link2"),
    ("email", "Mail"),
    ("emission", "CloudRain"),
    ("employee", "IdCard"),
    ("endpoint", "Webhook"),
    ("energy", "Zap"),
    ("engine", "Cog"),
    ("engineer", "HardHat"),
    ("enrollment", "ClipboardCheck"),
    ("environment", "Layers"),
    ("equipment", "Cog"),
    ("error", "Bug"),
    ("event", "Calendar"),
    ("eventcategory", "Activity"),
    ("evidence", "FileSearch"),
    ("exam", "FileCheck"),
    ("expense", "TrendingDown"),
    ("experiment", "TestTube"),
    ("eye", "Eye"),
    ("facility", "Building2"),
    ("factory", "Factory"),
    ("failure", "AlertTriangle"),
    ("failuremode", "TriangleAlert"),
    ("family", "UsersRound"),
    ("fan", "Fan"),
    ("fault", "AlertTriangle"),
    ("feed", "Rss"),
    ("file", "File"),
    ("filter", "Filter"),
    ("financialrequest", "BadgeIndianRupee"),
    ("financialresponse", "ReceiptIndianRupee"),
    ("fir", "FileWarning"),
    ("firsection", "BookMarked"),
    ("firstinformationreport", "FileWarning"),
    ("fish", "Fish"),
    ("flight", "Plane"),
    ("forensicrequest", "Microscope"),
    ("forensicresult", "TestTube"),
    ("form", "ClipboardList"),
    ("formsubmission", "ClipboardCheck"),
    ("fraud", "AlertTriangle"),
    ("fuel", "Fuel"),
    ("fund", "HandCoins"),
    ("furnace", "Flame"),
    ("gauge", "Gauge"),
    ("gene", "Dna"),
    ("generator", "Zap"),
    ("genome", "Dna"),
    ("grade", "Award"),
    ("hash", "Binary"),
    ("headquarters", "Building2"),
    ("heater", "Flame"),
    ("hierarchy", "ListTree"),
    ("hospital", "Hospital"),
    ("host", "Server"),
    ("hotel", "Hotel"),
    ("household", "Home"),
    ("idea", "Lightbulb"),
    ("identity", "Fingerprint"),
    ("image", "Image"),
    ("incident", "Siren"),
    ("incidentcategory", "Siren"),
    ("insight", "Sparkles"),
    ("inspection", "Scan"),
    ("institution", "GraduationCap"),
    ("instrument", "Activity"),
    ("instrumenttool", "Hammer"),
    ("intervention", "Pill"),
    ("inverter", "Plug"),
    ("investigatingofficer", "UserSearch"),
    ("investigationcase", "FolderSearch"),
    ("invoice", "Receipt"),
    ("ipaddress", "Network"),
    ("isotope", "Radiation"),
    ("job", "Zap"),
    ("joborder", "ClipboardCheck"),
    ("journal", "Book"),
    ("judge", "Gavel"),
    ("jurisdiction", "Map"),
    ("label", "Tag"),
    ("lawsuit", "Gavel"),
    ("ledger", "ClipboardList"),
    ("lesson", "BookOpen"),
    ("licence", "Stamp"),
    ("license", "Stamp"),
    ("lien", "Snowflake"),
    ("line", "Workflow"),
    ("loan", "Banknote"),
    ("location", "MapPin"),
    ("log", "Terminal"),
    ("loss", "IndianRupee"),
    ("lossitem", "IndianRupee"),
    ("lot", "Layers"),
    ("machine", "Cog"),
    ("machinery", "Cog"),
    ("maintenance", "Wrench"),
    ("match", "Calendar"),
    ("measurement", "Gauge"),
    ("medal", "Medal"),
    ("medication", "Pill"),
    ("member", "UserRound"),
    ("merchant", "Store"),
    ("message", "MessageSquare"),
    ("meter", "Gauge"),
    ("metric", "Gauge"),
    ("module", "Puzzle"),
    ("molecule", "Atom"),
    ("motor", "Cog"),
    ("network", "Network"),
    ("nodalofficer", "UserCog"),
    ("node", "Circle"),
    ("notification", "Send"),
    ("nurse", "Stethoscope"),
    ("offence", "ShieldAlert"),
    ("offense", "ShieldAlert"),
    ("office", "Briefcase"),
    ("officer", "BadgeCheck"),
    ("operator", "HardHat"),
    ("order", "ShoppingCart"),
    ("organisation", "Building2"),
    ("organization", "Building2"),
    ("outage", "PowerOff"),
    ("outcome", "Target"),
    ("pallet", "Forklift"),
    ("panel", "CircuitBoard"),
    ("paper", "FileText"),
    ("part", "Blocks"),
    ("pathway", "GitBranch"),
    ("patient", "HeartPulse"),
    ("payment", "CreditCard"),
    ("period", "Hourglass"),
    ("permission", "Lock"),
    ("permit", "FileCheck"),
    ("person", "User"),
    ("phone", "Phone"),
    ("photo", "Camera"),
    ("pipe", "Waypoints"),
    ("pipeline", "CircuitBoard"),
    ("plant", "Factory"),
    ("plantspecies", "TreePine"),
    ("player", "User"),
    ("policestation", "Shield"),
    ("policeunit", "ShieldCheck"),
    ("policy", "Scroll"),
    ("port", "Ship"),
    ("power", "Zap"),
    ("premises", "Home"),
    ("price", "DollarSign"),
    ("priority", "Flame"),
    ("probe", "Activity"),
    ("procedure", "ClipboardList"),
    ("process", "Workflow"),
    ("product", "Package"),
    ("productcategory", "Layers"),
    ("protein", "Dna"),
    ("protocol", "ClipboardList"),
    ("provisionallien", "Snowflake"),
    ("publication", "BookOpen"),
    ("pump", "Waves"),
    ("qrcode", "QrCode"),
    ("queue", "Layers"),
    ("rack", "Grid3x3"),
    ("raredisease", "HeartPulse"),
    ("rating", "Star"),
    ("reading", "LineChart"),
    ("record", "Files"),
    ("recording", "Mic"),
    ("refund", "HandCoins"),
    ("region", "Globe"),
    ("registry", "Library"),
    ("remittance", "ArrowLeftRight"),
    ("reopenrequest", "RefreshCw"),
    ("repair", "Wrench"),
    ("report", "FileText"),
    ("repository", "GitBranch"),
    ("request", "ClipboardList"),
    ("researcher", "Microscope"),
    ("reservoir", "Waves"),
    ("responselineitem", "List"),
    ("revenue", "TrendingUp"),
    ("risk", "AlertTriangle"),
    ("riskcategory", "ShieldAlert"),
    ("roadblock", "TrafficCone"),
    ("role", "UserCog"),
    ("route", "Route"),
    ("sample", "TestTube"),
    ("schedule", "Calendar"),
    ("school", "School"),
    ("score", "Target"),
    ("season", "Calendar"),
    ("sensor", "Activity"),
    ("serial", "Hash"),
    ("server", "Server"),
    ("service", "Cpu"),
    ("session", "Clock"),
    ("setpoint", "Target"),
    ("shape", "Shapes"),
    ("shift", "Clock"),
    ("ship", "Ship"),
    ("shipment", "Truck"),
    ("shop", "Store"),
    ("signal", "Antenna"),
    ("site", "MapPin"),
    ("solar", "Sun"),
    ("spare", "Blocks"),
    ("species", "Leaf"),
    ("specimen", "TestTube"),
    ("sponsor", "Briefcase"),
    ("staff", "Users"),
    ("stage", "Milestone"),
    ("state", "Map"),
    ("statement", "FileText"),
    ("stationstaffing", "Users"),
    ("statistic", "BarChart3"),
    ("step", "Milestone"),
    ("store", "Store"),
    ("student", "GraduationCap"),
    ("study", "Microscope"),
    ("supportreferral", "LifeBuoy"),
    ("suspect", "UserSearch"),
    ("symptom", "Activity"),
    ("tag", "Tag"),
    ("tank", "Droplet"),
    ("task", "ClipboardList"),
    ("tax", "Percent"),
    ("teacher", "School"),
    ("team", "Users"),
    ("technician", "HardHat"),
    ("telemetry", "RadioTower"),
    ("temperature", "Thermometer"),
    ("terrain", "Mountain"),
    ("therapeuticarea", "HeartPulse"),
    ("threat", "ShieldAlert"),
    ("threshold", "Ruler"),
    ("ticket", "Ticket"),
    ("timestamp", "Clock"),
    ("token", "Key"),
    ("tolerance", "Ruler"),
    ("tool", "Wrench"),
    ("tournament", "Trophy"),
    ("train", "Train"),
    ("transaction", "ArrowLeftRight"),
    ("transfer", "ArrowLeftRight"),
    ("transformer", "Zap"),
    ("transmitter", "RadioTower"),
    ("treatment", "Pill"),
    ("trend", "LineChart"),
    ("trial", "FlaskConical"),
    ("truck", "Truck"),
    ("turbine", "Fan"),
    ("type", "Shapes"),
    ("university", "GraduationCap"),
    ("url", "Link2"),
    ("user", "UserRound"),
    ("vaccine", "Syringe"),
    ("valve", "Split"),
    ("vehicle", "Car"),
    ("venue", "MapPin"),
    ("version", "History"),
    ("vessel", "Ship"),
    ("victim", "User"),
    ("victimcountry", "Globe"),
    ("video", "Video"),
    ("warehouse", "Warehouse"),
    ("waste", "Recycle"),
    ("water", "Droplet"),
    ("weather", "CloudRain"),
    ("website", "Globe"),
    ("witness", "User"),
    ("workorder", "ClipboardList"),
    ("zone", "Compass"),
];

pub(crate) const ICON_BY_KEYWORD: &[(&str, &str)] = &[
    ("clinicaltrial", "FlaskConical"),
    ("adverseevent", "Activity"),
    ("intervention", "Pill"),
    ("manufacturer", "Factory"),
    ("organisation", "Building2"),
    ("organization", "Building2"),
    ("publication", "BookOpen"),
    ("institution", "GraduationCap"),
    ("transaction", "ArrowLeftRight"),
    ("medication", "Pill"),
    ("university", "GraduationCap"),
    ("researcher", "Microscope"),
    ("regulation", "Scale"),
    ("repository", "GitBranch"),
    ("deployment", "Boxes"),
    ("department", "Building2"),
    ("identifier", "Hash"),
    ("diagnosis", "Stethoscope"),
    ("condition", "Stethoscope"),
    ("treatment", "Pill"),
    ("clinician", "Stethoscope"),
    ("warehouse", "Boxes"),
    ("inventory", "Boxes"),
    ("territory", "Globe"),
    ("compound", "FlaskConical"),
    ("molecule", "FlaskConical"),
    ("specimen", "FlaskConical"),
    ("citation", "BookOpen"),
    ("abstract", "FileText"),
    ("protocol", "ClipboardList"),
    ("shipment", "Truck"),
    ("supplier", "Factory"),
    ("customer", "UserRound"),
    ("merchant", "Store"),
    ("category", "Layers"),
    ("contract", "FileText"),
    ("database", "Database"),
    ("endpoint", "Network"),
    ("facility", "Building2"),
    ("employee", "User"),
    ("schedule", "Calendar"),
    ("document", "File"),
    ("transfer", "ArrowLeftRight"),
    ("category", "Layers"),
    ("disease", "HeartPulse"),
    ("symptom", "Activity"),
    ("protein", "Dna"),
    ("pathway", "GitBranch"),
    ("patient", "UserRound"),
    ("outcome", "Target"),
    ("therapy", "Pill"),
    ("article", "FileText"),
    ("journal", "BookOpen"),
    ("dataset", "Database"),
    ("sponsor", "Briefcase"),
    ("product", "Package"),
    ("invoice", "Receipt"),
    ("payment", "CreditCard"),
    ("lawsuit", "Gavel"),
    ("licence", "FileText"),
    ("license", "FileText"),
    ("cluster", "Layers"),
    ("service", "Cpu"),
    ("airport", "Plane"),
    ("vehicle", "Car"),
    ("railway", "Train"),
    ("station", "MapPin"),
    ("contact", "User"),
    ("account", "WalletCards"),
    ("genome", "Dna"),
    ("cohort", "Users"),
    ("dosage", "Pill"),
    ("author", "User"),
    ("vendor", "Store"),
    ("policy", "Scale"),
    ("server", "Server"),
    ("flight", "Plane"),
    ("vessel", "Ship"),
    ("region", "Globe"),
    ("period", "Calendar"),
    ("report", "FileText"),
    ("record", "FileText"),
    ("trial", "FlaskConical"),
    ("study", "Microscope"),
    ("paper", "FileText"),
    ("grant", "Receipt"),
    ("order", "Receipt"),
    ("store", "Store"),
    ("brand", "Tag"),
    ("court", "Gavel"),
    ("group", "Users"),
    ("event", "Calendar"),
    ("label", "Tag"),
    ("class", "Layers"),
    ("drug", "Pill"),
    ("gene", "Dna"),
    ("site", "MapPin"),
    ("team", "Users"),
    ("file", "File"),
    ("code", "Hash"),
    ("type", "Layers"),
    ("tag", "Tag"),
];

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn labels(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_valid_style_reads_without_warnings() {
        let s = IconStyle::read(&json!({
            "format": FORMAT,
            "nodes": {"Country": {"color": "#2563EB", "icon": "lucide:Flag", "caption": "name"}},
            "edges": {"IN_REGION": {"color": "#94A3B8"}}
        }));
        assert!(s.warnings.is_empty(), "{:?}", s.warnings);
        assert_eq!(s.nodes["Country"].icon.as_deref(), Some("lucide:Flag"));
        assert_eq!(s.edges["IN_REGION"].color.as_deref(), Some("#94A3B8"));
    }

    #[test]
    fn a_bad_field_is_dropped_and_the_rest_kept() {
        let s = IconStyle::read(&json!({
            "format": FORMAT,
            "nodes": {
                "A": {"color": "red", "icon": "lucide:Bug"},
                "B": {"color": "#12345"},
                "C": {"icon": "<svg/>"}
            },
            "edges": {"R": {"color": 7}}
        }));
        assert_eq!(s.nodes.len(), 1);
        assert_eq!(
            s.nodes["A"],
            NodeStyle {
                color: None,
                icon: Some("lucide:Bug".into()),
                caption: None
            }
        );
        assert!(s.edges.is_empty());
        assert_eq!(s.warnings.len(), 7, "{:?}", s.warnings);
    }

    #[test]
    fn an_unknown_format_is_ignored_whole() {
        let s = IconStyle::read(
            &json!({"format": "samyama.icons/9", "nodes": {"A": {"color": "#000000"}}}),
        );
        assert!(s.nodes.is_empty());
        assert_eq!(s.warnings.len(), 1);
    }

    #[test]
    fn deserializing_never_fails() {
        let s: IconStyle = serde_json::from_value(json!(42)).unwrap();
        assert!(s.is_empty());
        assert_eq!(s.warnings.len(), 1);
    }

    #[test]
    fn too_many_entries_are_cut_off() {
        let nodes: serde_json::Map<String, Value> = (0..MAX_ENTRIES + 3)
            .map(|i| (format!("L{i:05}"), json!({"color": "#000000"})))
            .collect();
        let s = IconStyle::read(&json!({"format": FORMAT, "nodes": nodes}));
        assert_eq!(s.nodes.len(), MAX_ENTRIES);
        assert_eq!(s.warnings.len(), 1);
    }

    #[test]
    fn strict_refuses_what_read_would_skip() {
        assert!(IconStyle::strict(&json!({"nodes": {"A": {"color": "#000000"}}})).is_ok());
        let err = IconStyle::strict(&json!({"nodes": {"A": {"colour": "#000000"}}})).unwrap_err();
        assert!(err.iter().any(|w| w.contains("colour")), "{err:?}");
    }

    #[test]
    fn import_reports_and_drops_unknown_labels() {
        let s = IconStyle::read(&json!({
            "format": FORMAT,
            "nodes": {"A": {"color": "#000000"}, "Gone": {"color": "#000000"}},
            "edges": {"R": {"color": "#000000"}, "OLD": {"color": "#000000"}}
        }));
        let (kept, report) = IconStyle::for_import(Some(&s), &labels(&["A", "B"]), &labels(&["R"]));
        let kept = kept.unwrap();
        assert_eq!(kept.nodes.keys().collect::<Vec<_>>(), ["A"]);
        assert_eq!(kept.edges.keys().collect::<Vec<_>>(), ["R"]);
        assert_eq!(report.source, "header");
        assert_eq!(report.styled, 1);
        assert_eq!(report.unstyled, ["B"]);
        assert_eq!(report.unknown_labels, ["Gone"]);
        assert_eq!(report.unknown_edge_types, ["OLD"]);

        let (none, report) = IconStyle::for_import(None, &labels(&["A"]), &[]);
        assert!(none.is_none());
        assert_eq!(report.source, "none");
        assert_eq!(report.unstyled, ["A"]);
    }

    /// The same numbers Insight's `hashSlot` gives (checked in node).
    #[test]
    fn hash_slot_matches_insight() {
        assert_eq!(hash_slot("Person", 10), 5);
        assert_eq!(hash_slot("Disease", 10), 8);
        assert_eq!(hash_slot("", 10), 0);
    }

    #[test]
    fn initial_keeps_existing_entries_and_fills_the_rest() {
        let existing =
            IconStyle::read(&json!({"format": FORMAT, "nodes": {"Person": {"color": "#000000"}}}));
        let s = IconStyle::initial(&labels(&["Person", "BankAccount", "Zzz"]), Some(&existing));
        assert_eq!(s.nodes["Person"].color.as_deref(), Some("#000000"));
        assert_eq!(
            s.nodes["BankAccount"].icon.as_deref(),
            Some("lucide:WalletCards")
        );
        assert!(s.nodes["BankAccount"]
            .color
            .as_deref()
            .unwrap()
            .starts_with('#'));
        assert_eq!(s.nodes["Zzz"].icon, None);
    }

    #[test]
    fn default_icons_follow_insight() {
        assert_eq!(default_icon("Person"), Some("User"));
        assert_eq!(default_icon("bank_account"), Some("WalletCards"));
        assert_eq!(default_icon("RareDisease"), Some("HeartPulse"));
        assert_eq!(default_icon("Qwerty"), None);
    }
}
